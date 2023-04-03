-- this sql query allows you to modify the instanceId of the column if it is different from the one contained in the payload
-- this sql query works for payloads in expand format
-- if the key of the json path does not exist it returns a null
-- and the postgres operator returns false if one of the two parameters is null (https://www.postgresql.org/docs/current/functions-comparison.html)
update attribute_instance
set instance_id = jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id'
where instance_id != jsonb_path_query_first(payload, '$."https://uri.etsi.org/ngsi-ld/instanceId"[0]')->>'@id';

-- this sql query is the same as above but for non expandable formats (version before v2)
update attribute_instance
set instance_id = jsonb_path_query_first(payload, '$')->>'instanceId'
where instance_id != jsonb_path_query_first(payload, '$')->>'instanceId';

-- the following sql queries allow to modify the instanceId of the audit table
-- this allows to no longer have similar instanceId between the two attributes instances tables
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
