ALTER TABLE temporal_entity_attribute ADD COLUMN dataset_id VARCHAR(255);
ALTER TABLE temporal_entity_attribute DROP CONSTRAINT entity_pkey;
ALTER TABLE temporal_entity_attribute
    ADD CONSTRAINT temporal_entity_attribute_uniqueness
    UNIQUE (entity_id, attribute_name, dataset_id);