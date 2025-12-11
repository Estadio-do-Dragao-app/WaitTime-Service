"""
MQTT Event Consumer for Wait Time Service
- Subscribes to DOWNSTREAM broker (receives events from simulator)
- Publishes to UPSTREAM broker (sends wait times to clients)
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
    Consumes queue events from DOWNSTREAM Mosquitto broker
    Publishes wait time updates to UPSTREAM Mosquitto broker
    """
    
    def __init__(self, window_minutes: int = 5):
        self.window_minutes = window_minutes
        self.running = False
        
        # Two separate MQTT clients
        self.downstream_client = None  # For receiving events
        self.upstream_client = None    # For publishing updates
        
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
        """Start consuming events"""
        self.running = True
        
        # Run both connections concurrently
        await asyncio.gather(
            self._run_downstream(),
            self._run_upstream_keepalive()
        )
    
    async def _run_downstream(self):
        """Connect to downstream broker and consume events"""
        while self.running:
            try:
                logger.info(
                    f"Connecting to DOWNSTREAM broker: "
                    f"{settings.DOWNSTREAM_BROKER_HOST}:{settings.DOWNSTREAM_BROKER_PORT}"
                )
                
                async with aiomqtt.Client(
                    hostname=settings.DOWNSTREAM_BROKER_HOST,
                    port=settings.DOWNSTREAM_BROKER_PORT,
                    identifier=f"waittime-downstream-{id(self)}"
                ) as client:
                    self.downstream_client = client
                    
                    # Subscribe to queue events
                    await client.subscribe(settings.DOWNSTREAM_TOPIC_QUEUES)
                    logger.info(f"Subscribed to: {settings.DOWNSTREAM_TOPIC_QUEUES}")
                    
                    await client.subscribe(settings.DOWNSTREAM_TOPIC_ALL)
                    logger.info(f"Subscribed to: {settings.DOWNSTREAM_TOPIC_ALL}")
                    
                    logger.info("DOWNSTREAM Consumer started - waiting for messages...")
                    
                    # Process incoming messages
                    async for message in client.messages:
                        await self._on_message(message)
                        
            except aiomqtt.MqttError as e:
                logger.error(f"DOWNSTREAM connection error: {e}")
                self.stats['errors'] += 1
                if self.running:
                    logger.info("Reconnecting DOWNSTREAM in 5 seconds...")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"DOWNSTREAM unexpected error: {e}")
                self.stats['errors'] += 1
                if self.running:
                    await asyncio.sleep(5)
    
    async def _run_upstream_keepalive(self):
        """Keep upstream connection alive for publishing"""
        while self.running:
            try:
                logger.info(
                    f"Connecting to UPSTREAM broker: "
                    f"{settings.UPSTREAM_BROKER_HOST}:{settings.UPSTREAM_BROKER_PORT}"
                )
                
                async with aiomqtt.Client(
                    hostname=settings.UPSTREAM_BROKER_HOST,
                    port=settings.UPSTREAM_BROKER_PORT,
                    identifier=f"waittime-upstream-{id(self)}"
                ) as client:
                    self.upstream_client = client
                    logger.info("UPSTREAM connection established")
                    
                    # Keep connection alive
                    while self.running:
                        await asyncio.sleep(1)
                        
            except aiomqtt.MqttError as e:
                logger.error(f"UPSTREAM connection error: {e}")
                if self.running:
                    logger.info("Reconnecting UPSTREAM in 5 seconds...")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"UPSTREAM unexpected error: {e}")
                if self.running:
                    await asyncio.sleep(5)
    
    async def _on_message(self, message: aiomqtt.Message):
        """Process incoming message from downstream broker"""
        try:
            self.stats['messages_received'] += 1
            
            topic = str(message.topic)
            payload = message.payload.decode()
            
            logger.debug(f"Received on {topic}: {payload[:100]}...")
            
            try:
                event_data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON: {payload[:50]}")
                return
            
            event_type = event_data.get('event_type', '')
            
            if event_type == 'queue_update':
                await self._process_queue_event(event_data)
            elif event_type == 'gate_passage':
                await self._process_gate_event(event_data)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.stats['errors'] += 1
    
    async def _process_queue_event(self, event_data: dict):
        """Process queue update and calculate wait time"""
        facility_type = event_data.get('facility_type', '')
        facility_id = event_data.get('facility_id', '')
        queue_length = event_data.get('queue_length', 0)
        capacity = event_data.get('capacity', 10)
        
        poi_id = self._convert_facility_id(facility_type, facility_id)
        if not poi_id:
            return
        
        try:
            async with get_db() as db:
                poi_repo = POIRepository(db)
                waittime_repo = WaitTimeRepository(db)
                
                poi = await poi_repo.get_poi_by_id(poi_id)
                
                num_servers = poi.num_servers if poi else (4 if facility_type == 'BAR' else 8)
                service_rate = poi.service_rate if poi else (0.4 if facility_type == 'BAR' else 0.5)
                
                arrival_rate = queue_length / (self.window_minutes or 1)
                smoother = self.smoothers[poi_id]
                smoothed_rate = smoother.update(arrival_rate)
                
                if poi_id not in self.queue_models:
                    self.queue_models[poi_id] = QueueModel(num_servers=num_servers)
                
                model = self.queue_models[poi_id]
                result = model.calculate_wait_time(
                    arrival_rate=smoothed_rate,
                    service_rate=service_rate,
                    sample_count=queue_length
                )
                
                previous_state = await waittime_repo.get_queue_state_raw(poi_id)
                significant_change = self._is_significant_change(
                    previous_state.get('wait_minutes') if previous_state else None,
                    result.wait_minutes
                )
                
                await waittime_repo.update_queue_state(
                    poi_id=poi_id,
                    arrival_rate=smoothed_rate,
                    wait_minutes=result.wait_minutes,
                    confidence_lower=result.confidence_lower,
                    confidence_upper=result.confidence_upper,
                    sample_count=queue_length,
                    status=result.status
                )
                
                if significant_change or not previous_state:
                    await self._publish_waittime_update(
                        poi_id=poi_id,
                        wait_minutes=result.wait_minutes,
                        confidence_lower=result.confidence_lower,
                        confidence_upper=result.confidence_upper,
                        status=result.status,
                        queue_length=queue_length
                    )
                
                logger.info(f"Updated {poi_id}: wait={result.wait_minutes:.1f}min, queue={queue_length}")
                
        except Exception as e:
            logger.error(f"Error processing queue event: {e}")
            self.stats['errors'] += 1
    
    async def _process_gate_event(self, event_data: dict):
        """Process gate passage events"""
        gate_id = event_data.get('gate', '')
        direction = event_data.get('direction', 'entry')
        logger.debug(f"Gate event: {gate_id} - {direction}")
    
    def _convert_facility_id(self, facility_type: str, facility_id: str) -> Optional[str]:
        """Convert simulator facility ID to Map-Service POI ID format"""
        if not facility_id:
            return None
        
        parts = facility_id.lower().replace('-', '_').split('_')
        if len(parts) < 2:
            return facility_id
        
        type_map = {'bar': 'Food', 'toilet': 'WC', 'wc': 'WC', 'restroom': 'WC'}
        direction_map = {
            'norte': 'Norte', 'sul': 'Sul', 'este': 'Este', 'oeste': 'Oeste',
            'north': 'Norte', 'south': 'Sul', 'east': 'Este', 'west': 'Oeste'
        }
        
        poi_type = type_map.get(parts[0], parts[0].title())
        direction = direction_map.get(parts[1], parts[1].title()) if len(parts) > 1 else ''
        number = parts[-1] if len(parts) > 2 and parts[-1].isdigit() else '1'
        
        if poi_type == 'WC':
            return f"{poi_type}-{direction}-L0-{number}"
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
        """Publish wait time update to UPSTREAM broker"""
        if not self.upstream_client:
            logger.warning("UPSTREAM client not connected")
            return
        
        update = {
            "type": "waittime",
            "poi": poi_id,
            "minutes": round(wait_minutes, 1),
            "ci95": [round(confidence_lower, 1), round(confidence_upper, 1)],
            "status": status,
            "queue_length": queue_length,
            "ts": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            topic = f"{settings.UPSTREAM_TOPIC_PREFIX}/{poi_id}"
            await self.upstream_client.publish(topic, json.dumps(update))
            self.stats['messages_published'] += 1
            logger.debug(f"Published to UPSTREAM: {topic}")
        except Exception as e:
            logger.error(f"Failed to publish: {e}")
            self.stats['errors'] += 1
    
    def _is_significant_change(self, old_wait: Optional[float], new_wait: float) -> bool:
        if old_wait is None:
            return True
        if old_wait == 0:
            return new_wait > 0.5
        return abs((new_wait - old_wait) / old_wait * 100) >= 15.0
    
    async def stop(self):
        self.running = False
        logger.info("MQTT Consumer stopped")
    
    def get_stats(self) -> dict:
        return self.stats


# Backwards compatibility
EventConsumer = MQTTEventConsumer
