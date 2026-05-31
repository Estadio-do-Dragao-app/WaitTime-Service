-- Indices para otimizar queries de WaitTime-Service
-- Executar após init_db()

-- POI table indices
CREATE INDEX IF NOT EXISTS idx_poi_type ON pois(poi_type);
CREATE INDEX IF NOT EXISTS idx_poi_created_at ON pois(created_at);

-- CameraEvent table indices (idx_poi_timestamp já existe)
CREATE INDEX IF NOT EXISTS idx_camera_event_type ON camera_events(event_type);
CREATE INDEX IF NOT EXISTS idx_camera_id ON camera_events(camera_id);

-- QueueState table indices (ótimo para queries frequentes)
CREATE INDEX IF NOT EXISTS idx_queue_status ON queue_states(status);
CREATE INDEX IF NOT EXISTS idx_queue_last_updated ON queue_states(last_updated);

-- Composite indices para queries comuns
CREATE INDEX IF NOT EXISTS idx_camera_poi_timestamp ON camera_events(poi_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_camera_poi_type ON camera_events(poi_id, event_type);

-- Analisar tabelas para otimizar query planner
ANALYZE pois;
ANALYZE camera_events;
ANALYZE queue_states;
