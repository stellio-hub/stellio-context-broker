update entity_access_rights
set subject_id = (
    select service_account_id
    from subject_referential
    where subject_referential.subject_id = entity_access_rights.subject_id
)
where subject_id in (select subject_id from subject_referential where subject_type = 'CLIENT')
