"""
MQTT Event Consumer for Wait Time Service
Subscribes to queue events from Mosquitto broker (from simulator)
Publishes waittime updates back to Mosquitto
"""
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict

import aiomqtt

from models import QueueEvent, WaitTimeUpdate
from queueModel import QueueModel, ArrivalRateSmoother
from db.database import get_db
from db.repositories import WaitTimeRepository, POIRepository, CameraEventRepository
from config.config import settings

logger = logging.getLogger(__name__)


class MQTTEventConsumer:
    """
    Consumes queue events from Mosquitto MQTT broker
    Calculates wait times and publishes updates
    """
    
    def __init__(self, window_minutes: int = 5):
        """
        Args:
            window_minutes: Time window for calculating arrival rate
        """
        self.window_minutes = window_minutes
        self.running = False
        
        # MQTT client (will be created in connect)
        self.client = None
        
        # Per-POI smoothers for arrival rates
        self.smoothers = defaultdict(lambda: ArrivalRateSmoother(alpha=0.3))
        
        # Queue models per POI (cached)
        self.queue_models = {}
        
        # Stats
        self.stats = {
            'messages_received': 0,
            'messages_published': 0,
            'errors': 0
        }
    
    async def start(self):
        """Start consuming events from MQTT broker"""
        self.running = True
        
        while self.running:
            try:
                logger.info(f"Connecting to MQTT broker: {settings.MQTT_BROKER_HOST}:{settings.MQTT_BROKER_PORT}")
                
                async with aiomqtt.Client(
                    hostname=settings.MQTT_BROKER_HOST,
                    port=settings.MQTT_BROKER_PORT,
                    identifier=f"waittime-service-{id(self)}"
                ) as client:
                    self.client = client
                    
                    # Subscribe to queue events topic
                    await client.subscribe(settings.MQTT_TOPIC_QUEUES)
                    logger.info(f"Subscribed to: {settings.MQTT_TOPIC_QUEUES}")
                    
                    # Also subscribe to all events for additional context
                    await client.subscribe(settings.MQTT_TOPIC_ALL_EVENTS)
                    logger.info(f"Subscribed to: {settings.MQTT_TOPIC_ALL_EVENTS}")
                    
                    logger.info("MQTT Consumer started - waiting for messages...")
                    
                    # Process incoming messages
                    async for message in client.messages:
                        await self._on_message(message)
                        
            except aiomqtt.MqttError as e:
                logger.error(f"MQTT connection error: {e}")
                self.stats['errors'] += 1
                if self.running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                self.stats['errors'] += 1
                if self.running:
                    await asyncio.sleep(5)
    
    async def _on_message(self, message: aiomqtt.Message):
        """
        Callback for incoming MQTT messages
        
        Expected message format from simulator (queue events):
        {
            "event_type": "queue_update",
            "facility_type": "BAR" or "TOILET",
            "facility_id": "bar_norte_1",
            "location": [x, y],
            "queue_length": 5,
            "capacity": 20,
            "wait_time_estimate": 3.5,
            "timestamp": "2025-12-11T17:00:00Z"
        }
        """
        try:
            self.stats['messages_received'] += 1
            
            topic = str(message.topic)
            payload = message.payload.decode()
            
            logger.debug(f"Received message on {topic}: {payload[:100]}...")
            
            # Parse JSON
            try:
                event_data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON on {topic}: {payload[:50]}")
                return
            
            # Process based on event type
            event_type = event_data.get('event_type', '')
            
            if event_type == 'queue_update':
                await self._process_queue_event(event_data)
            elif event_type == 'gate_passage':
                # Gate events can be used for capacity tracking
                await self._process_gate_event(event_data)
            else:
                # Log unknown event types for debugging
                logger.debug(f"Ignoring event type: {event_type}")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.stats['errors'] += 1
    
    async def _process_queue_event(self, event_data: dict):
        """
        Process queue update event and calculate wait time
        """
        facility_type = event_data.get('facility_type', '')
        facility_id = event_data.get('facility_id', '')
        queue_length = event_data.get('queue_length', 0)
        capacity = event_data.get('capacity', 10)
        simulator_estimate = event_data.get('wait_time_estimate', 0)
        
        # Create POI ID matching Map-Service format
        # Simulator uses: bar_norte_1, wc_sul_2, etc.
        # Map-Service uses: WC-Norte-L0-1, Food-Norte-1, etc.
        poi_id = self._convert_facility_id(facility_type, facility_id)
        
        if not poi_id:
            logger.debug(f"Could not map facility: {facility_type}/{facility_id}")
            return
        
        try:
            async with get_db() as db:
                poi_repo = POIRepository(db)
                waittime_repo = WaitTimeRepository(db)
                
                # Get POI configuration
                poi = await poi_repo.get_poi_by_id(poi_id)
                
                if not poi:
                    # Use defaults if POI not found
                    num_servers = 4 if facility_type == 'BAR' else 8
                    service_rate = 0.4 if facility_type == 'BAR' else 0.5
                else:
                    num_servers = poi.num_servers or 4
                    service_rate = poi.service_rate or 0.5
                
                # Calculate arrival rate from queue length change
                # This is a simplified approach - in production you'd track entries over time
                arrival_rate = queue_length / (self.window_minutes or 1)
                
                # Apply EMA smoothing
                smoother = self.smoothers[poi_id]
                smoothed_rate = smoother.update(arrival_rate)
                
                # Get or create queue model
                if poi_id not in self.queue_models:
                    self.queue_models[poi_id] = QueueModel(num_servers=num_servers)
                
                model = self.queue_models[poi_id]
                
                # Calculate wait time using M/M/k model
                result = model.calculate_wait_time(
                    arrival_rate=smoothed_rate,
                    service_rate=service_rate,
                    sample_count=queue_length
                )
                
                # Get previous wait time
                previous_state = await waittime_repo.get_queue_state_raw(poi_id)
                significant_change = self._is_significant_change(
                    previous_state.get('wait_minutes') if previous_state else None,
                    result.wait_minutes
                )
                
                # Update database
                await waittime_repo.update_queue_state(
                    poi_id=poi_id,
                    arrival_rate=smoothed_rate,
                    wait_minutes=result.wait_minutes,
                    confidence_lower=result.confidence_lower,
                    confidence_upper=result.confidence_upper,
                    sample_count=queue_length,
                    status=result.status
                )
                
                # Publish update if significant change
                if significant_change or not previous_state:
                    await self._publish_waittime_update(
                        poi_id=poi_id,
                        wait_minutes=result.wait_minutes,
                        confidence_lower=result.confidence_lower,
                        confidence_upper=result.confidence_upper,
                        status=result.status,
                        queue_length=queue_length
                    )
                
                logger.info(
                    f"Updated {poi_id}: wait={result.wait_minutes:.1f}min, "
                    f"queue={queue_length}, status={result.status}"
                )
                
        except Exception as e:
            logger.error(f"Error processing queue event for {poi_id}: {e}")
            self.stats['errors'] += 1
    
    async def _process_gate_event(self, event_data: dict):
        """Process gate passage events for capacity tracking"""
        # Gate events from simulator:
        # {"event_type": "gate_passage", "gate": "portao_norte", "person_id": 1, "direction": "entry"}
        
        gate_id = event_data.get('gate', '')
        direction = event_data.get('direction', 'entry')
        
        logger.debug(f"Gate event: {gate_id} - {direction}")
        # Could be used for capacity tracking in future
    
    def _convert_facility_id(self, facility_type: str, facility_id: str) -> Optional[str]:
        """
        Convert simulator facility ID to Map-Service POI ID format
        
        Simulator: bar_norte_1, wc_sul_2, toilet_este_1
        Map-Service: Food-Norte-1, WC-Norte-L0-1
        """
        if not facility_id:
            return None
        
        # Parse simulator ID (e.g., "bar_norte_1", "toilet_sul_2")
        parts = facility_id.lower().replace('-', '_').split('_')
        
        if len(parts) < 2:
            return facility_id  # Return as-is if can't parse
        
        # Map facility types
        type_map = {
            'bar': 'Food',
            'toilet': 'WC',
            'wc': 'WC',
            'restroom': 'WC'
        }
        
        # Map directions
        direction_map = {
            'norte': 'Norte',
            'sul': 'Sul',
            'este': 'Este',
            'oeste': 'Oeste',
            'north': 'Norte',
            'south': 'Sul',
            'east': 'Este',
            'west': 'Oeste'
        }
        
        poi_type = type_map.get(parts[0], parts[0].title())
        direction = direction_map.get(parts[1], parts[1].title()) if len(parts) > 1 else ''
        number = parts[-1] if len(parts) > 2 and parts[-1].isdigit() else '1'
        
        # Format: WC-Norte-L0-1 or Food-Norte-1
        if poi_type == 'WC':
            return f"{poi_type}-{direction}-L0-{number}"
        else:
            return f"{poi_type}-{direction}-{number}"
    
    async def _publish_waittime_update(
        self,
        poi_id: str,
        wait_minutes: float,
        confidence_lower: float,
        confidence_upper: float,
        status: str,
        queue_length: int = 0
    ):
        """
        Publish wait time update to MQTT
        
        Message format:
        {
            "type": "waittime",
            "poi": "WC-Norte-L0-1",
            "minutes": 6.0,
            "ci95": [4.0, 8.5],
            "status": "medium",
            "queue_length": 5,
            "ts": "2025-12-11T17:06:00Z"
        }
        """
        if not self.client:
            logger.warning("MQTT client not connected, skipping publish")
            return
        
        update = {
            "type": "waittime",
            "poi": poi_id,
            "minutes": round(wait_minutes, 1),
            "ci95": [
                round(confidence_lower, 1),
                round(confidence_upper, 1)
            ],
            "status": status,
            "queue_length": queue_length,
            "ts": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            topic = f"{settings.MQTT_TOPIC_WAITTIME}/{poi_id}"
            payload = json.dumps(update, ensure_ascii=False)
            
            await self.client.publish(topic, payload)
            self.stats['messages_published'] += 1
            
            logger.debug(f"Published waittime update to {topic}")
            
        except Exception as e:
            logger.error(f"Failed to publish waittime update: {e}")
            self.stats['errors'] += 1
    
    def _is_significant_change(
        self,
        old_wait: Optional[float],
        new_wait: float,
        threshold_percent: float = 15.0
    ) -> bool:
        """Check if wait time changed significantly"""
        if old_wait is None:
            return True
        
        if old_wait == 0:
            return new_wait > 0.5  # Any wait > 30s is significant
        
        percent_change = abs((new_wait - old_wait) / old_wait * 100)
        return percent_change >= threshold_percent
    
    async def stop(self):
        """Stop consuming"""
        self.running = False
        logger.info("MQTT Consumer stopped")
    
    def get_stats(self) -> dict:
        """Get consumer statistics"""
        return self.stats


# Backwards compatibility alias
EventConsumer = MQTTEventConsumer
