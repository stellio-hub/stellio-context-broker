-- Create specific_access_policy column in entity_payload
alter table entity_payload add column specific_access_policy varchar(16);

-- Move existing specific_access_policy column to entity_payload table
UPDATE entity_payload
SET specific_access_policy = (
    SELECT specific_access_policy
    FROM temporal_entity_attribute
    WHERE entity_payload.entity_id = temporal_entity_attribute.entity_id
    LIMIT 1
    );

-- Drop existing specific_access_policy column in temporal_entity_attribute
alter table temporal_entity_attribute drop specific_access_policy;