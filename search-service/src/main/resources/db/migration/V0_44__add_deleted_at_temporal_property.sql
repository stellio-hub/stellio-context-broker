ALTER TABLE temporal_entity_attribute
ADD COLUMN deleted_at timestamp with time zone;

ALTER TABLE entity_payload
ADD COLUMN deleted_at timestamp with time zone;
