ALTER TABLE entity_temporal_property RENAME TO temporal_entity_attribute;

-- Add pgcryto extension to use UUID generation function
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

ALTER TABLE temporal_entity_attribute ADD COLUMN id uuid UNIQUE DEFAULT gen_random_uuid();
ALTER TABLE temporal_entity_attribute ADD COLUMN entity_payload jsonb;

-- Migrate data in observation table
ALTER TABLE observation RENAME TO attribute_instance;
ALTER TABLE attribute_instance ADD COLUMN instance_id uuid DEFAULT gen_random_uuid();
ALTER TABLE attribute_instance RENAME COLUMN value TO measured_value;
ALTER TABLE attribute_instance ADD COLUMN value VARCHAR(2048);
ALTER TABLE attribute_instance ADD COLUMN temporal_entity_attribute uuid;
ALTER TABLE attribute_instance DROP COLUMN unit_code;

-- Set references to the temporal_entity_attribute table
UPDATE attribute_instance SET temporal_entity_attribute =
    (SELECT id FROM temporal_entity_attribute
        WHERE attribute_instance.attribute_name = temporal_entity_attribute.attribute_name
        AND attribute_instance.observed_by = temporal_entity_attribute.observed_by);

DELETE FROM _timescaledb_catalog.dimension WHERE column_name = 'observed_by';
SELECT add_dimension('attribute_instance', 'temporal_entity_attribute', 2);

-- We can now remove the observed_by data
ALTER TABLE temporal_entity_attribute DROP COLUMN observed_by;
ALTER TABLE attribute_instance DROP COLUMN observed_by;
ALTER TABLE attribute_instance DROP COLUMN attribute_name;

DROP TABLE property_observation;

-- Primary and foreign keys

ALTER TABLE attribute_instance ADD FOREIGN KEY (temporal_entity_attribute) REFERENCES temporal_entity_attribute(id);
ALTER TABLE attribute_instance ADD CONSTRAINT instance_unicity UNIQUE (temporal_entity_attribute, observed_at);
