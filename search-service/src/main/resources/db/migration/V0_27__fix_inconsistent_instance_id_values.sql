update attribute_instance
set instance_id = trim('"' FROM (payload->'instanceId')::text)
where instance_id != trim('"' FROM (payload->'instanceId')::text);
