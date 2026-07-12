ALTER TABLE entity_payload
    ADD COLUMN IF NOT EXISTS expires_at timestamp with time zone;

ALTER TABLE temporal_entity_attribute
    ADD COLUMN IF NOT EXISTS expires_at timestamp with time zone;

CREATE INDEX IF NOT EXISTS idx_entity_payload_expires_at
    ON entity_payload (expires_at) WHERE expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_temporal_entity_attribute_expires_at
    ON temporal_entity_attribute (expires_at) WHERE expires_at IS NOT NULL;
