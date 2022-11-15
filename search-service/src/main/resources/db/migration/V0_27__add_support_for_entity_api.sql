alter table entity_payload
    add column types text[],
    add column created_at timestamp with time zone,
    add column modified_at timestamp with time zone,
    add column contexts text[],
    add column specific_access_policy varchar(16);

-- migrate specific access policy from tea to entity_payload
-- in some cases, specific access policy is missing for some attributes of a given entity (newer ones?)
-- so let's just take the max value to be sure we have one if there is one
update entity_payload
    set specific_access_policy = (
        select max(specific_access_policy)
        from temporal_entity_attribute
        where entity_payload.entity_id = temporal_entity_attribute.entity_id
        limit 1
    );

-- migrate types from tea to entity_payload
update entity_payload
    set types = (
        select types
        from temporal_entity_attribute
        where entity_payload.entity_id = temporal_entity_attribute.entity_id
        limit 1
    );

-- extract data from payloads stored in entity_payload
update entity_payload
    set created_at = (jsonb_path_query_first(payload, '$.createdAt')::text)::timestamp with time zone,
        modified_at =  (jsonb_path_query_first(payload, '$.modifiedAt')::text)::timestamp with time zone,
        contexts = ARRAY(select jsonb_path_query_first(payload, '$."@context"'));

-- columns will be populated with the 0.28 programmatic migration script
alter table temporal_entity_attribute
    drop column types,
    drop column specific_access_policy,
    add column created_at timestamp with time zone,
    add column modified_at timestamp with time zone,
    add column payload jsonb;

update temporal_entity_attribute
    set attribute_value_type = 'NUMBER'
    where attribute_value_type = 'MEASURE';

update temporal_entity_attribute
    set attribute_value_type = 'STRING'
    where attribute_value_type = 'ANY';

alter table temporal_entity_attribute
    alter column attribute_value_type set default 'NUMBER';

alter table temporal_entity_attribute
    alter column id drop default;

delete from temporal_entity_attribute
    where attribute_name = 'https://ontology.eglobalmark.com/authorization#specificAccessPolicy';

alter table attribute_instance
    add column geo_value geometry;

alter table attribute_instance_audit
    add column geo_value geometry;

-- TODO populate with existing data
alter table subject_referential
    add column subject_info jsonb;
