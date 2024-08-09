ALTER TABLE temporal_entity_attribute RENAME TO attribute;
ALTER TABLE attribute_instance RENAME COLUMN temporal_entity_attribute TO attribute;
ALTER TABLE attribute_instance_audit RENAME COLUMN temporal_entity_attribute TO attribute;