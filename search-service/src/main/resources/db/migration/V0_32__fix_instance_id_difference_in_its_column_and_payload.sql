-- we modify the instancesId of the column if it is different from the one is contained in the payload
-- this sql query works for expanded payload
-- if the result of jsonb_path_query_first is null the operation returns false (https://www.postgresql.org/docs/current/functions-comparison.html)
update attribute_instance
set instance_id = jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id'
where instance_id != jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id';

-- this sql query is the same as above but for not expanded payload (version before v2)
update attribute_instance
set instance_id = jsonb_path_query_first(payload, '$')->>'instanceId'
where instance_id != jsonb_path_query_first(payload, '$')->>'instanceId';

-- Currently the two attributes instances tables have the same instanceId. We fix this to only have different instanceId
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
