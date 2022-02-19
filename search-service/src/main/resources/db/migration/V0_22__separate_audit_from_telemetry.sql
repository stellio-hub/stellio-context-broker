-- create the audit table

create table attribute_instance_audit
(
    time                      timestamp with time zone not null,
    measured_value            double precision,
    value                     varchar(4096),
    temporal_entity_attribute uuid not null
        references temporal_entity_attribute (id) on delete cascade,
    instance_id               varchar(255) not null,
    payload                   jsonb,
    time_property             varchar(20),
    constraint instance_audit_unicity
        unique (temporal_entity_attribute, time, time_property)
);

create index attribute_instance_audit_observed_at_idx
    on attribute_instance_audit (time desc);

create index attribute_instance_audit_tea_time_property_idx
    on attribute_instance_audit (temporal_entity_attribute, time_property);

SELECT create_hypertable('attribute_instance_audit','time', chunk_time_interval => INTERVAL '13 weeks');

-- move some data from telemetry table to audit table

insert into attribute_instance_audit
    (time, measured_value, value, temporal_entity_attribute, instance_id, payload, time_property)
    select time, measured_value, value, temporal_entity_attribute, instance_id, payload, time_property
        from attribute_instance
        where (time_property = 'CREATED_AT' or time_property = 'MODIFIED_AT');

delete from attribute_instance
    where (time_property = 'CREATED_AT' or time_property = 'MODIFIED_AT');

-- restore telemetry table to its previous state

alter table attribute_instance drop column time_property;
ALTER TABLE attribute_instance DROP CONSTRAINT if exists instance_unicity;
ALTER TABLE attribute_instance ADD CONSTRAINT instance_unicity UNIQUE (temporal_entity_attribute, time);

DROP INDEX if exists attribute_instance_temporal_entity_attribute_time_property_idx;
CREATE INDEX if not exists attribute_instance_temporal_entity_attribute_idx
    ON attribute_instance(temporal_entity_attribute);

-- add the new cascade constraints

alter table attribute_instance
    drop constraint attribute_instance_temporal_entity_attribute_fkey,
    add foreign key (temporal_entity_attribute)
        references temporal_entity_attribute(id)
        on delete cascade;

-- remove the currently unused lat / lon columns

alter table attribute_instance drop column latitude;
alter table attribute_instance drop column longitude;
