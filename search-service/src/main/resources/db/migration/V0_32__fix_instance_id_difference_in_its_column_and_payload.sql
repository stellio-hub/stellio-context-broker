update attribute_instance
set instance_id = jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id'
where instance_id != jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id';

update attribute_instance
set instance_id = jsonb_path_query_first(payload, '$')->>'instanceId'
where instance_id != jsonb_path_query_first(payload, '$')->>'instanceId';

update attribute_instance_audit
set instance_id = concat('urn:ngsi-ld:Instance:', gen_random_uuid());

update attribute_instance_audit
set payload =
    case
        when instance_id != jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id'
            then jsonb_set(payload, '{"https://uri.etsi.org/ngsi-ld/instanceId",0,"@id"}', to_jsonb(instance_id))
        when instance_id != jsonb_path_query_first(payload, '$')->>'instanceId'
            then jsonb_set(payload, '{instanceId}', to_jsonb(instance_id))
        else payload
    end;
