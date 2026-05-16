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
import threading

import paho.mqtt.client as mqtt

from schemas import QueueEvent, WaitTimeUpdate
from queueModel import QueueModel, ArrivalRateSmoother
from db.database import get_db
from db.repositories import WaitTimeRepository, POIRepository
from db.schemas import POI
from config.config import settings

logger = logging.getLogger(__name__)

class RobustMQTTConsumer:
    """
    Consumes queue events from DOWNSTREAM Mosquitto broker
    Publishes wait time updates to UPSTREAM Mosquitto broker
    Uses paho-mqtt with background threads
    """
    
    def __init__(self, window_minutes: int = 5):
        self.window_minutes = window_minutes
        self.running = False
        self.loop = asyncio.get_event_loop()
        
        # Two MQTT clients: one for receiving, one for sending
        self.downstream_client = mqtt.Client(client_id=f"waittime-downstream-{id(self)}")
        self.upstream_client = mqtt.Client(client_id=f"waittime-upstream-{id(self)}")
        
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
        
        # Connect to UPSTREAM broker (for publishing wait times to clients)
        logger.info(f"Connecting to UPSTREAM: {settings.UPSTREAM_BROKER_HOST}:{settings.UPSTREAM_BROKER_PORT}")
        try:
            self.upstream_client.connect(settings.UPSTREAM_BROKER_HOST, settings.UPSTREAM_BROKER_PORT, 60)
            self.upstream_client.loop_start()
            logger.info("[UPSTREAM] Connected and loop started")
        except Exception:
            logger.exception("[UPSTREAM] Failed to connect")
        
        # Connect to DOWNSTREAM broker (for receiving events from simulator)
        while self.running:
            try:
                logger.info(f"Connecting to DOWNSTREAM: {settings.DOWNSTREAM_BROKER_HOST}:{settings.DOWNSTREAM_BROKER_PORT}")
                self.downstream_client.connect(settings.DOWNSTREAM_BROKER_HOST, settings.DOWNSTREAM_BROKER_PORT, 60)
                self.downstream_client.loop_start()
                logger.info("[DOWNSTREAM] Connected and loop started")
                
                while self.running:
                    await asyncio.sleep(1)
                    
            except Exception:
                logger.exception("DOWNSTREAM connection error")
                self.stats['errors'] += 1
                if self.running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)


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
        """Handle incoming message with robust error handling"""
        try:
            payload = msg.payload.decode('utf-8')
            topic = msg.topic
            logger.info(f"[MQTT] Received message on topic: {topic}")
            logger.debug(f"[MQTT] Payload: {payload}")

            try:
                event_data = json.loads(payload)
            except json.JSONDecodeError:
                logger.exception(f"Malformed JSON received on {topic}")
                return

            if topic == settings.DOWNSTREAM_TOPIC_QUEUES:
                logger.info(f"[MQTT] Processing QueueEvent from topic: {topic}")
                try:
                    event = QueueEvent.model_validate(event_data)
                    asyncio.run_coroutine_threadsafe(
                        self._process_queue_event(event), 
                        self.loop
                    )
                except Exception:
                    logger.exception("Pydantic validation failed for QueueEvent")
                    return

            elif topic == settings.DOWNSTREAM_TOPIC_ALL:
                logger.debug(f"Received metadata event on {topic}")

        except Exception:
            logger.exception("Critical error in MQTT callback")

    # --- Async Processors ---

    async def _process_queue_event(self, event: QueueEvent):
        """Process queue update and calculate wait time"""
        if not self.running:
            return

        facility_type = event.location_type
        facility_id = event.location_id
        queue_length = event.queue_length
        
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
                
                # Arrival rate calculation with EMA smoothing
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
                self._is_significant_change(
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
                
                self._publish_waittime_update(
                    poi_id=poi_id,
                    wait_minutes=result.wait_minutes,
                    confidence_lower=result.confidence_lower,
                    confidence_upper=result.confidence_upper,
                    status=result.status,
                    queue_length=queue_length
                )
                
                logger.info(f"Updated {poi_id}: wait={result.wait_minutes:.1f}min, queue={queue_length}")
                
                if poi_id == "POI-cantina":
                    await self._handle_cantina_reconciliation(
                        poi_repo, waittime_repo,
                        num_servers, service_rate,
                        smoothed_rate, result, queue_length
                    )
                
        except Exception:
            logger.exception("Error processing queue event")
            self.stats['errors'] += 1

    async def _handle_cantina_reconciliation(
        self, poi_repo, waittime_repo,
        num_servers, service_rate,
        smoothed_rate, result, queue_length
    ):
        """
        Propagate wait time from POI-cantina to alternate IDs used in the Fanapp graph.
        Ensures entries for 'Cantina de Santiago' in the app reflect simulation data.
        """
        alternate_map = {
            "POI-1870236080": "Cantina de Santiago - Universidade de Aveiro",
            "POI-652293975": "Cantina de Santiago"
        }
        for alt_id, alt_name in alternate_map.items():
            existing_poi = await poi_repo.get_poi_by_id(alt_id)
            if not existing_poi:
                await poi_repo.session.merge(POI(
                    id=alt_id,
                    name=alt_name,
                    poi_type="food",
                    num_servers=num_servers,
                    service_rate=service_rate
                ))
                await poi_repo.session.commit()

            self._publish_waittime_update(
                poi_id=alt_id,
                wait_minutes=result.wait_minutes,
                confidence_lower=result.confidence_lower,
                confidence_upper=result.confidence_upper,
                status=result.status,
                queue_length=queue_length
            )
            await waittime_repo.update_queue_state(
                poi_id=alt_id,
                arrival_rate=smoothed_rate,
                wait_minutes=result.wait_minutes,
                confidence_lower=result.confidence_lower,
                confidence_upper=result.confidence_upper,
                sample_count=queue_length,
                status=result.status
            )

    def _convert_facility_id(self, facility_type: str, facility_id: str):
        if not facility_id:
            return None
        
        # Already a standard POI ID from the graph
        if facility_id.startswith('POI-'):
            return facility_id

        parts = facility_id.lower().replace('-', '_').split('_')
        if len(parts) < 2:
            return facility_id
        
        type_map = {'bar': 'Food', 'toilet': 'WC', 'wc': 'WC', 'restroom': 'WC'}
        direction_map = {
            'norte': 'Norte', 'sul': 'Sul', 'este': 'Este', 'oeste': 'Oeste',
            'north': 'Norte', 'south': 'Sul', 'east': 'Este', 'west': 'Oeste'
        }
        
        poi_type = type_map.get(parts[0], parts[0].title())
        direction = direction_map.get(parts[1], parts[1].title())
        number = parts[-1] if len(parts) > 2 and parts[-1].isdigit() else '1'
        
        if poi_type == 'WC':
            return f"{poi_type}-{direction}-L0-{number}"
        return f"{poi_type}-{direction}-{number}"

    def _publish_waittime_update(
        self, poi_id, wait_minutes, confidence_lower, confidence_upper, status, queue_length=0
    ):
        """Publish wait time update via paho-mqtt (thread-safe)"""
        update = {
            "type": "waittime",
            "poi": poi_id,
            "minutes": round(wait_minutes, 1),
            "ci95": [round(confidence_lower, 1), round(confidence_upper, 1)],
            "status": status,
            "queue_length": queue_length,
            "ts": datetime.now(timezone.utc).isoformat(),
            "priority": "NORMAL",
            "expiry_time": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        }
        
        try:
            topic = f"{settings.UPSTREAM_TOPIC_PREFIX}/{poi_id}"
            self.upstream_client.publish(topic, json.dumps(update))
            self.stats['messages_published'] += 1
            logger.info(f"[UPSTREAM] Published to topic: {topic} (wait={wait_minutes:.1f}min, queue={queue_length})")
        except Exception:
            logger.exception("[UPSTREAM] Failed to publish")
            self.stats['errors'] += 1
    
    def _is_significant_change(self, old_wait: Optional[float], new_wait: float) -> bool:
        if old_wait is None:
            return True
        if old_wait == 0:
            return new_wait > 0.5
        return abs((new_wait - old_wait) / old_wait * 100) >= 15.0

    def get_stats(self) -> dict:
        return self.stats

# Backwards compatibility alias
EventConsumer = RobustMQTTConsumer
