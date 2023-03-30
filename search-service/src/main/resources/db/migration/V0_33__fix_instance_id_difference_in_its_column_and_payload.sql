update attribute_instance
set instance_id = trim('"' FROM jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]."@id"')::text)
where instance_id != trim('"' FROM jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]."@id"')::text);
