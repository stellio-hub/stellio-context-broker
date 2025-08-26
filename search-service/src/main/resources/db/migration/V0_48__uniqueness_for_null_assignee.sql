alter table permission
    drop constraint entity_access_rights_uniqueness;

WITH duplicates AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY assignee, action, target_id
               ORDER BY id
               ) AS rn
    FROM permission
)
DELETE FROM permission
WHERE id IN (
    SELECT id FROM duplicates WHERE rn > 1
);

alter table permission
    add constraint permission_uniqueness
        unique NULLS NOT DISTINCT (assignee, action, target_id);