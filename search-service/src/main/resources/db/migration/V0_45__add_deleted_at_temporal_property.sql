ALTER TABLE temporal_entity_attribute
    ADD COLUMN deleted_at timestamp with time zone;

ALTER TABLE entity_payload
    ADD COLUMN deleted_at timestamp with time zone;

DROP INDEX IF EXISTS temporal_entity_attribute_null_datasetid_uniqueness;

ALTER TABLE temporal_entity_attribute
    DROP CONSTRAINT temporal_entity_attribute_uniqueness;

ALTER TABLE temporal_entity_attribute
    ADD CONSTRAINT temporal_entity_attribute_uniqueness UNIQUE NULLS NOT DISTINCT (entity_id, attribute_name, dataset_id);
