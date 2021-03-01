ALTER TABLE attribute_instance ADD COLUMN payload jsonb;
-- Migrate default instances
UPDATE attribute_instance SET payload = json_build_object('type','Property','value',measured_value,'observedAt',to_char(observed_at, 'YYYY-MM-DDThh24:MI:SS.USZ'),'instanceId',instance_id)::jsonb FROM temporal_entity_attribute WHERE attribute_instance.temporal_entity_attribute = temporal_entity_attribute.id AND measured_value IS NOT NULL AND dataset_id IS NULL;
UPDATE attribute_instance SET payload = json_build_object('type','Property','value',value,'observedAt',to_char(observed_at, 'YYYY-MM-DDThh24:MI:SS.USZ'),'instanceId',instance_id)::jsonb FROM temporal_entity_attribute WHERE attribute_instance.temporal_entity_attribute = temporal_entity_attribute.id AND value IS NOT NULL AND dataset_id IS NULL;

-- Migrate instances with datasetId
UPDATE attribute_instance SET payload = json_build_object('type','Property','datasetId',dataset_id,'value',measured_value,'observedAt',to_char(observed_at, 'YYYY-MM-DDThh24:MI:SS.USZ'),'instanceId',instance_id)::jsonb FROM temporal_entity_attribute WHERE attribute_instance.temporal_entity_attribute = temporal_entity_attribute.id AND measured_value IS NOT NULL AND dataset_id IS NOT NULL;
UPDATE attribute_instance SET payload = json_build_object('type','Property','datasetId',dataset_id,'value',value,'observedAt',to_char(observed_at, 'YYYY-MM-DDThh24:MI:SS.USZ'),'instanceId',instance_id)::jsonb FROM temporal_entity_attribute WHERE attribute_instance.temporal_entity_attribute = temporal_entity_attribute.id AND value IS NOT NULL AND dataset_id IS NOT NULL;
