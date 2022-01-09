alter table subject_referential
    alter column subject_id type varchar(256) using subject_id::varchar;
alter table subject_referential
    alter column service_account_id type varchar(256) using service_account_id::varchar;
alter table entity_access_rights
    alter column subject_id type varchar(256) using subject_id::varchar;
