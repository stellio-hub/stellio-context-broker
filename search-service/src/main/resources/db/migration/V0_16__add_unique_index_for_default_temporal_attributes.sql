CREATE UNIQUE INDEX temporal_entity_attribute_null_datasetid_uniqueness
ON temporal_entity_attribute (entity_id, attribute_name, (dataset_id IS NULL)) WHERE dataset_id IS NULL;
