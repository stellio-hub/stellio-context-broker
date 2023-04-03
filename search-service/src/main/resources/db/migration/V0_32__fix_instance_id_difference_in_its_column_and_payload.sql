-- modify the value of the instance_id column if different from the instance_id value contained in the payload
-- jsonb_path_query_first returns null if the key doesn't exist
-- and the operator "!=" returns null (not true or false) when either input is null (https://www.postgresql.org/docs/current/functions-comparison.html)
-- this query works for expanded payloads
update attribute_instance
set instance_id = jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id'
where instance_id != jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id';

-- same query as above but for compacted payloads (version before v2)
update attribute_instance
set instance_id = jsonb_path_query_first(payload, '$')->>'instanceId'
where instance_id != jsonb_path_query_first(payload, '$')->>'instanceId';

-- the two attributes instances tables have the same instanceId for some updates of attributes
-- to prevent from any inconsistency, reset all the values of instanceId in audit table
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
