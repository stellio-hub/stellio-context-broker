alter table permission
    drop constraint entity_access_rights_uniqueness;

alter table permission
    add constraint permission_uniqueness
        unique NULLS NOT DISTINCT (assignee, action, target_id);