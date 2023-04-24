update entity_payload
set types = ARRAY(SELECT jsonb_array_elements_text(jsonb_path_query(payload, '$."@type"')))
where types is null;
