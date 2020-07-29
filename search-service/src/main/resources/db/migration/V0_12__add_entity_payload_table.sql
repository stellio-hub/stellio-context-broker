-- Create entity_payload table
CREATE TABLE entity_payload(
    entity_id       VARCHAR(255) NOT NULL PRIMARY KEY,
    payload jsonb NULL
);

-- Move existing entity_payload rows to entity_payload table
INSERT INTO entity_payload
SELECT DISTINCT entity_id, entity_payload
FROM temporal_entity_attribute;

-- Drop existing entity_payload column in temporal_entity_attribute
ALTER TABLE temporal_entity_attribute DROP entity_payload;