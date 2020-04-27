ALTER TABLE attribute_instance ADD COLUMN instance_id_uri varchar(255);
UPDATE attribute_instance SET instance_id_uri = 'urn:ngsi-ld:Instance:' || instance_id::text;
ALTER TABLE attribute_instance DROP COLUMN instance_id;
ALTER TABLE attribute_instance RENAME COLUMN instance_id_uri TO instance_id;
ALTER TABLE attribute_instance ALTER COLUMN instance_id SET NOT NULL;
