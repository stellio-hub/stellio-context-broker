ALTER TABLE attribute_instance ADD COLUMN metadata jsonb;
UPDATE attribute_instance SET metadata = json_build_object('type','Property','value',measured_value,'observedAt',observed_at)::jsonb WHERE measured_value IS NOT NULL ;
UPDATE attribute_instance SET metadata = json_build_object('type','Property','value',value,'observedAt',observed_at)::jsonb WHERE value IS NOT NULL ;
