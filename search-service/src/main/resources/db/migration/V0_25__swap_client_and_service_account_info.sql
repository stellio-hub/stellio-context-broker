-- since newly created entities have been manually synchronized with the sync endpoint
-- there are duplicated entities (one row with client id, another one with service account id)
-- thus first remove duplicates under the service account id key
delete from entity_access_rights as A
where subject_id in (select service_account_id from subject_referential)
and exists (
    select subject_id
    from entity_access_rights
    where subject_id in (select subject_id from subject_referential where subject_type = 'CLIENT')
    and A.entity_id = entity_access_rights.entity_id
);

-- then update rows referencing client id to a reference to a service account id
update entity_access_rights
set subject_id = (
    select service_account_id
    from subject_referential
    where subject_referential.subject_id = entity_access_rights.subject_id
)
where subject_id in (select subject_id from subject_referential where subject_type = 'CLIENT');
