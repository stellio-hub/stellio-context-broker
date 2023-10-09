alter table attribute_instance
    alter column instance_id type text,
    alter column value type text;

alter table attribute_instance_audit
    alter column instance_id type text,
    alter column value type text;

alter table entity_access_rights
    alter column subject_id type text,
    alter column access_right type text,
    alter column entity_id type text;

alter table entity_payload
    alter column entity_id type text,
    alter column specific_access_policy type text;

alter table subject_referential
    alter column subject_id type text,
    alter column subject_type type text,
    alter column service_account_id type text;

alter table temporal_entity_attribute
    alter column entity_id type text,
    alter column attribute_name type text,
    alter column attribute_value_type type text,
    alter column dataset_id type text,
    alter column attribute_type type text;
