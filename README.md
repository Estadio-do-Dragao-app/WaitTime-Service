### WaitTime-Service

# Recived events:

- **Type: Queue_Update**
- Info:
    
    * queue_id
    * nº people in queue
    * ts(timestamp)


## event generator:

```bash
def generate_queue_event(self, location_type, location_id, location, queue_length, avg_service_time=None):
        """Evento de fila de espera (FAN APP - wait times)"""
        if avg_service_time is None:
            avg_service_time = 2.0 if location_type == "TOILET" else 3.5
        
        wait_time_min = queue_length * avg_service_time if queue_length > 0 else 0
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "queue_update",
            "timestamp": datetime.now().isoformat() + "Z",
            "location_type": location_type,
            "location_id": location_id,
            "location": {"x": float(location[0]), "y": float(location[1])},
            "queue_length": queue_length,
        }
        self.events.append(event)
        self.mqtt_publisher.publish_event(event)
        self.event_count += 1
        return event
```