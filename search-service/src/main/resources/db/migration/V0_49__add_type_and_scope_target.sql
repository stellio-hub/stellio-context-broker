alter table permission
    add target_types text[];

alter table permission
    add target_scopes text[];

alter table permission
    alter column target_id drop not null;

alter table permission
    drop constraint permission_uniqueness;

alter table permission
    add constraint permission_uniqueness
        unique NULLS NOT DISTINCT (assignee, action, target_id, target_types, target_scopes);