update entity_payload
set modified_at = created_at
where modified_at is null;

alter table entity_payload
    alter column modified_at set not null;

update context_source_registration
set modified_at = created_at
where modified_at is null;

alter table context_source_registration
    alter column modified_at set not null;

