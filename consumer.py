"""
MQTT Event Consumer for Wait Time Service
- Subscribes to DOWNSTREAM broker (receives events from simulator)
- Publishes to UPSTREAM broker (sends wait times to clients)
Replaced aiomqtt (async) with paho-mqtt (sync) for compatibility
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
import json
from collections import defaultdict
import threading

import paho.mqtt.client as mqtt

from models import QueueEvent, WaitTimeUpdate
from queueModel import QueueModel, ArrivalRateSmoother
from db.database import get_db
from db.repositories import WaitTimeRepository, POIRepository
from config.config import settings

logger = logging.getLogger(__name__)

class MQTTEventConsumer:
    """
    Consumes queue events from DOWNSTREAM Mosquitto broker
    Publishes wait time updates to UPSTREAM Mosquitto broker
    Uses paho-mqtt with background threads
    """
    
    def __init__(self, window_minutes: int = 5):
        self.window_minutes = window_minutes
        self.running = False
        self.loop = asyncio.get_event_loop()
        
        # Three separate MQTT clients
        self.downstream_client = mqtt.Client(client_id=f"waittime-downstream-{id(self)}")
        self.upstream_client = mqtt.Client(client_id=f"waittime-upstream-{id(self)}")
        self.client_publisher = mqtt.Client(client_id=f"waittime-client-pub-{id(self)}")
        
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

        # Force debug logging
        logger.setLevel(logging.DEBUG)

        # Configure callbacks
        self.downstream_client.on_connect = self._on_downstream_connect
        self.downstream_client.on_message = self._on_downstream_message
        self.downstream_client.enable_logger(logger)
        
        self.upstream_client.on_connect = self._on_upstream_connect
        self.upstream_client.enable_logger(logger)

    async def start(self):
        """Start consumption (connects and starts loops)"""
        self.running = True
        
        while self.running:
            try:
                # Connect Downstream
                logger.info(f"Connecting to DOWNSTREAM: {settings.DOWNSTREAM_BROKER_HOST}:{settings.DOWNSTREAM_BROKER_PORT}")
                if not self.downstream_client.is_connected():
                    self.downstream_client.connect(settings.DOWNSTREAM_BROKER_HOST, settings.DOWNSTREAM_BROKER_PORT, 60)
                    self.downstream_client.loop_start()
                
                # Connect Upstream
                logger.info(f"Connecting to UPSTREAM: {settings.UPSTREAM_BROKER_HOST}:{settings.UPSTREAM_BROKER_PORT}")
                if not self.upstream_client.is_connected():
                    self.upstream_client.connect(settings.UPSTREAM_BROKER_HOST, settings.UPSTREAM_BROKER_PORT, 60)
                    self.upstream_client.loop_start()
                
                break # Success
                
            except Exception as e:
                logger.error(f"Failed to start MQTT clients (retrying in 5s): {e}")
                await asyncio.sleep(5)

        # Monitor loop
        while self.running:
            await asyncio.sleep(1)

    async def stop(self):
        """Stop clients"""
        self.running = False
        self.downstream_client.loop_stop()
        self.upstream_client.loop_stop()
        logger.info("MQTT Consumer stopped")

    # --- Callbacks ---

    def _on_downstream_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("DOWNSTREAM Connected")
            client.subscribe(settings.DOWNSTREAM_TOPIC_QUEUES)
            logger.info(f"Subscribed to {settings.DOWNSTREAM_TOPIC_QUEUES}")
            
            client.subscribe(settings.DOWNSTREAM_TOPIC_ALL)
            logger.info(f"Subscribed to {settings.DOWNSTREAM_TOPIC_ALL}")
        else:
            logger.error(f"DOWNSTREAM Connection failed: {rc}")

    def _on_upstream_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("UPSTREAM Connected")
        else:
            logger.error(f"UPSTREAM Connection failed: {rc}")

    def _on_downstream_message(self, client, userdata, msg):
        """Handle incoming message - bridges to async loop"""
        try:
            payload = msg.payload.decode()
            topic = msg.topic
            
            logger.debug(f"Received on {topic}: {payload[:50]}...")
            self.stats['messages_received'] += 1
            
            # Parse JSON
            try:
                event_data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON received")
                return

            # Dispatch based on event type
            event_type = event_data.get('event_type', '')
            
            if event_type == 'queue_update':
                # Schedule async processing in the main loop
                asyncio.run_coroutine_threadsafe(
                    self._process_queue_event(event_data), 
                    self.loop
                )
            elif event_type == 'gate_passage':
                 asyncio.run_coroutine_threadsafe(
                    self._process_gate_event(event_data), 
                    self.loop
                )

        except Exception as e:
            logger.error(f"Error in message callback: {e}")

    # --- Async Processors (Logic copied from original) ---

    async def _process_queue_event(self, event_data: dict):
        """Process queue update and calculate wait time"""
        # Ensure we are not running if stopped
        if not self.running: return

        facility_type = event_data.get('location_type', '')
        facility_id = event_data.get('location_id', '')
        queue_length = event_data.get('queue_length', 0)
        # Default capacity if 0 or missing
        capacity = event_data.get('capacity', 10) or 10
        
        poi_id = self._convert_facility_id(facility_type, facility_id)
        if not poi_id:
            return
        
        try:
            async with get_db() as db:
                poi_repo = POIRepository(db)
                waittime_repo = WaitTimeRepository(db)
                
                poi = await poi_repo.get_poi_by_id(poi_id)
                
                # Defaults if POI not found in DB
                num_servers = poi.num_servers if poi else (4 if facility_type == 'BAR' else 8)
                service_rate = poi.service_rate if poi else (0.4 if facility_type == 'BAR' else 0.5)
                
                # Arrival Rate Calculation
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
                
                
                # Always publish to keep routing/fanapp updated
                self._publish_waittime_update(
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
        gate_id = event_data.get('gate', '')
        direction = event_data.get('direction', 'entry')
        logger.debug(f"Gate event: {gate_id} - {direction}")

    def _convert_facility_id(self, facility_type: str, facility_id: str):
        if not facility_id: return None
        
        parts = facility_id.lower().replace('-', '_').split('_')
        if len(parts) < 2: return facility_id
        
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

    def _publish_waittime_update(
        self, poi_id, wait_minutes, confidence_lower, confidence_upper, status, queue_length=0
    ):
        """Publish via paho-mqtt (thread-safe method)"""
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
            self.upstream_client.publish(topic, json.dumps(update))
            self.stats['messages_published'] += 1
            logger.debug(f"Published to UPSTREAM: {topic}")
        except Exception as e:
            logger.error(f"Failed to publish: {e}")

    def _is_significant_change(self, old_wait, new_wait) -> bool:
        if old_wait is None: return True
        if old_wait == 0: return new_wait > 0.5
        return abs((new_wait - old_wait) / old_wait * 100) >= 15.0

    def get_stats(self) -> dict:
        return self.stats

# Backwards compatibility
EventConsumer = MQTTEventConsumer
