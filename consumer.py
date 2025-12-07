"""
Event Consumer for Wait Time Service
Subscribes to queue_events from downstream broker
Publishes waittime_updates to upstream broker
"""
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict

import aio_pika
from aio_pika import Message, ExchangeType

from models import QueueEvent, WaitTimeUpdate
from queueModel import QueueModel, ArrivalRateSmoother
from db.database import get_db
from db.repositories import WaitTimeRepository, POIRepository, CameraEventRepository
from config.config import settings

logger = logging.getLogger(__name__)


class EventConsumer:
    """
    Consumes queue events from downstream broker
    Calculates wait times and publishes updates to upstream broker
    """
    
    def __init__(self, window_minutes: int = 5):
        """
        Args:
            window_minutes: Time window for calculating arrival rate
        """
        self.window_minutes = window_minutes
        self.running = False
        
        # RabbitMQ connections
        self.downstream_connection = None
        self.upstream_connection = None
        self.downstream_channel = None
        self.upstream_channel = None
        self.upstream_exchange = None
        
        # Per-POI smoothers for arrival rates
        self.smoothers = defaultdict(lambda: ArrivalRateSmoother(alpha=0.3))
        
        # Queue models per POI (cached)
        self.queue_models = {}
        
    async def connect(self):
        """Establish connections to both brokers"""
        try:
            # Connect to downstream broker (receives queue_events)
            logger.info(f"Connecting to downstream broker: {settings.DOWNSTREAM_BROKER_URL}")
            self.downstream_connection = await aio_pika.connect_robust(
                settings.DOWNSTREAM_BROKER_URL
            )
            self.downstream_channel = await self.downstream_connection.channel()
            await self.downstream_channel.set_qos(prefetch_count=10)
            
            # Connect to upstream broker (publishes waittime_updates)
            logger.info(f"Connecting to upstream broker: {settings.UPSTREAM_BROKER_URL}")
            self.upstream_connection = await aio_pika.connect_robust(
                settings.UPSTREAM_BROKER_URL
            )
            self.upstream_channel = await self.upstream_connection.channel()
            
            # Declare upstream exchange for publishing
            self.upstream_exchange = await self.upstream_channel.declare_exchange(
                settings.UPSTREAM_EXCHANGE,
                ExchangeType.TOPIC,
                durable=True
            )
            
            logger.info("Successfully connected to both brokers")
            
        except Exception as e:
            logger.error(f"Failed to connect to brokers: {e}")
            raise
    
    async def start(self):
        """Start consuming events from downstream broker"""
        if not self.downstream_channel:
            await self.connect()
        
        self.running = True
        
        try:
            # Declare and bind to queue_events queue
            queue = await self.downstream_channel.declare_queue(
                settings.QUEUE_EVENTS_QUEUE,
                durable=True
            )
            
            # Bind to exchange with routing key pattern
            exchange = await self.downstream_channel.declare_exchange(
                settings.DOWNSTREAM_EXCHANGE,
                ExchangeType.TOPIC,
                durable=True
            )
            
            await queue.bind(
                exchange,
                routing_key="queue.event.*"  # queue.event.entry, queue.event.exit
            )
            
            logger.info(f"Starting to consume from queue: {settings.QUEUE_EVENTS_QUEUE}")
            
            # Start consuming
            await queue.consume(self._on_message)
            
            # Keep running
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in event consumer: {e}")
            raise
    
    async def _on_message(self, message: aio_pika.IncomingMessage):
        """
        Callback for incoming messages from downstream broker
        
        Expected message format:
        {
            "poi_id": "Restroom-A3",
            "event_type": "entry",  // or "exit"
            "count": 1,
            "camera_id": "cam-101",
            "timestamp": "2025-10-08T18:05:00Z"
        }
        """
        async with message.process():
            try:
                # Parse event
                event_data = json.loads(message.body.decode())
                event = QueueEvent(**event_data)
                
                logger.debug(f"Received event: {event.poi_id} - {event.event_type}")
                
                # Store event in database
                async with get_db() as db:
                    camera_repo = CameraEventRepository(db)
                    await camera_repo.insert_event(event)
                
                # Calculate and publish wait time update
                await self._process_queue_event(event)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Message will be requeued if not acknowledged
    
    async def _process_queue_event(self, event: QueueEvent):
        """
        Process queue event and calculate wait time
        Publishes update to upstream broker if wait time changed significantly
        """
        async with get_db() as db:
            poi_repo = POIRepository(db)
            waittime_repo = WaitTimeRepository(db)
            camera_repo = CameraEventRepository(db)
            
            # Get POI configuration from database
            poi = await poi_repo.get_poi_by_id(event.poi_id)
            if not poi:
                logger.warning(f"POI {event.poi_id} not found in database""POI {event.poi_id} not found in database")
                return
            
            # Get recent events for rate calculation
            cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=self.window_minutes)
            recent_events = await camera_repo.get_events_since(event.poi_id, cutoff_time)
            
            if not recent_events:
                logger.debug(f"No recent events for {event.poi_id}")
                return
            
            # Calculate arrival rate
            arrival_rate = self._calculate_arrival_rate(recent_events, self.window_minutes)
            
            # Apply EMA smoothing
            smoother = self.smoothers[event.poi_id]
            smoothed_rate = smoother.update(arrival_rate)
            
            # Get or create queue model
            if event.poi_id not in self.queue_models:
                self.queue_models[event.poi_id] = QueueModel(num_servers=poi.num_servers)
            
            model = self.queue_models[event.poi_id]
            
            # Calculate wait time using M/M/k model
            result = model.calculate_wait_time(
                arrival_rate=smoothed_rate,
                service_rate=poi.service_rate,
                sample_count=len(recent_events)
            )
            
            # Get previous wait time to check if significant change
            previous_state = await waittime_repo.get_queue_state_raw(event.poi_id)
            significant_change = self._is_significant_change(
                previous_state.get('wait_minutes') if previous_state else None,
                result.wait_minutes
            )
            
            # Update database
            await waittime_repo.update_queue_state(
                poi_id=event.poi_id,
                arrival_rate=smoothed_rate,
                wait_minutes=result.wait_minutes,
                confidence_lower=result.confidence_lower,
                confidence_upper=result.confidence_upper,
                sample_count=len(recent_events),
                status=result.status
            )
            
            # Publish to upstream broker if significant change
            if significant_change or not previous_state:
                await self._publish_waittime_update(
                    poi_id=event.poi_id,
                    wait_minutes=result.wait_minutes,
                    confidence_lower=result.confidence_lower,
                    confidence_upper=result.confidence_upper,
                    status=result.status
                )
            
            logger.info(
                f"Updated {event.poi_id}: wait={result.wait_minutes:.2f}min, "
                f"rate={smoothed_rate:.2f}/min, status={result.status}, "
                f"published={significant_change}"
            )
    
    async def _publish_waittime_update(
        self,
        poi_id: str,
        wait_minutes: float,
        confidence_lower: float,
        confidence_upper: float,
        status: str
    ):
        """
        Publish wait time update to upstream broker
        
        Message format (matches project spec):
        {
            "type": "waittime",
            "poi": "Restroom-A3",
            "minutes": 6.0,
            "ci95": [4.0, 8.5],
            "status": "medium",
            "ts": "2025-10-08T18:06:00Z"
        }
        """
        if not self.upstream_exchange:
            logger.warning("Upstream exchange not available, skipping publish")
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
            "ts": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            message = Message(
                body=json.dumps(update).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            
            # Publish with routing key: waittime.update.{poi_id}
            routing_key = f"waittime.update.{poi_id}"
            
            await self.upstream_exchange.publish(
                message,
                routing_key=routing_key
            )
            
            logger.debug(f"Published waittime update: {routing_key}")
            
        except Exception as e:
            logger.error(f"Failed to publish waittime update: {e}")
    
    def _calculate_arrival_rate(self, events: list, window_minutes: int) -> float:
        """Calculate arrival rate from recent events"""
        total_entries = sum(
            event.count for event in events 
            if event.event_type == 'entry'
        )
        
        return total_entries / window_minutes if window_minutes > 0 else 0.0
    
    def _is_significant_change(
        self,
        old_wait: Optional[float],
        new_wait: float,
        threshold_percent: float = 15.0
    ) -> bool:
        """
        Check if wait time changed significantly
        
        Args:
            old_wait: Previous wait time in minutes
            new_wait: New wait time in minutes
            threshold_percent: Minimum percent change to be significant
        """
        if old_wait is None:
            return True
        
        if old_wait == 0:
            return new_wait > 0.5  # If was 0, any wait > 30s is significant
        
        percent_change = abs((new_wait - old_wait) / old_wait * 100)
        return percent_change >= threshold_percent
    
    async def stop(self):
        """Stop consuming and close connections"""
        self.running = False
        
        if self.downstream_channel:
            await self.downstream_channel.close()
        if self.downstream_connection:
            await self.downstream_connection.close()
        
        if self.upstream_channel:
            await self.upstream_channel.close()
        if self.upstream_connection:
            await self.upstream_connection.close()
        
        logger.info("Event consumer stopped")
