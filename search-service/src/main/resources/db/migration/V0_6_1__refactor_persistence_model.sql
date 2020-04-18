ALTER TABLE entity_temporal_property RENAME TO temporal_entity_attribute;

ALTER TABLE temporal_entity_attribute ADD COLUMN id uuid UNIQUE DEFAULT gen_random_uuid();
ALTER TABLE temporal_entity_attribute ADD COLUMN entity_payload jsonb;

-- Set references to the temporal_entity_attribute table
ALTER TABLE observation ADD COLUMN temporal_entity_attribute uuid;
UPDATE observation SET temporal_entity_attribute =
    (SELECT id FROM temporal_entity_attribute
        WHERE observation.attribute_name = temporal_entity_attribute.attribute_name
        AND observation.observed_by = temporal_entity_attribute.observed_by);
    
-- Migrate data in the new attribute_instance table
CREATE TABLE attribute_instance (
    observed_at timestamp with time zone NOT NULL,
    measured_value double precision,
    latitude double precision,
    longitude double precision,
    instance_id uuid DEFAULT gen_random_uuid(),
    value character varying(2048),
    temporal_entity_attribute uuid NOT NULL
);

INSERT INTO attribute_instance
    (observed_at, measured_value, latitude, longitude, instance_id, temporal_entity_attribute)
    (SELECT observed_at, value, latitude, longitude, gen_random_uuid(), temporal_entity_attribute FROM observation);

SELECT create_hypertable('attribute_instance', 'observed_at', migrate_data => true);

-- We can now remove the observed_by data
ALTER TABLE temporal_entity_attribute DROP COLUMN observed_by;

DROP TABLE property_observation;
DROP TABLE observation;

-- Primary and foreign keys

ALTER TABLE attribute_instance ADD FOREIGN KEY (temporal_entity_attribute) REFERENCES temporal_entity_attribute(id);
ALTER TABLE attribute_instance ADD CONSTRAINT instance_unicity UNIQUE (temporal_entity_attribute, observed_at);
