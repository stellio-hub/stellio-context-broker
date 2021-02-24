ALTER TABLE attribute_instance ADD COLUMN payload jsonb;
UPDATE attribute_instance SET payload = json_build_object('type','Property','value',measured_value,'observedAt',to_char(observed_at, 'YYYY-MM-DDThh24:MI:SS.USZ'))::jsonb WHERE measured_value IS NOT NULL ;
UPDATE attribute_instance SET payload = json_build_object('type','Property','value',value,'observedAt',to_char(observed_at, 'YYYY-MM-DDThh24:MI:SS.USZ'))::jsonb WHERE value IS NOT NULL ;
