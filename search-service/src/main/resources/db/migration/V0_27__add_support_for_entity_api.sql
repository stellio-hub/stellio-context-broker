alter table attribute_instance
    add column geo_value geometry;

alter table attribute_instance_audit
    add column geo_value geometry;

alter table entity_payload
    add column types text[],
    add column created_at timestamp with time zone,
    add column modified_at timestamp with time zone,
    add column contexts text[];

-- migrate types from tea to entity_payload
update entity_payload
    set types = (
        select types
        from temporal_entity_attribute
        where entity_payload.entity_id = temporal_entity_attribute.entity_id
        limit 1
    );

alter table temporal_entity_attribute
    drop column types,
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
