update entity_payload
set modified_at = created_at
where modified_at is null;

update context_source_registration
set modified_at = created_at
where modified_at is null;
