update subject_referential
    set subject_id = service_account_id
    where service_account_id is not null
    and subject_type = 'CLIENT';

alter table subject_referential
    drop column service_account_id;
