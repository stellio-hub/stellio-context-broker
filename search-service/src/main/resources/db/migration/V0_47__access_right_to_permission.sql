ALTER TABLE entity_access_rights
    RENAME TO permission;

ALTER TABLE permission
    RENAME COLUMN subject_id TO assignee;

ALTER TABLE permission
    RENAME COLUMN access_right TO action;

ALTER TABLE permission
    RENAME COLUMN entity_id TO target_id;

ALTER TABLE permission
    ADD COLUMN id           text,
    ADD COLUMN assigner     text,
    ADD COLUMN created_at   timestamp with time zone,
    ADD COLUMN modified_at  timestamp with time zone;


-- migrate specific access policy into permission
INSERT INTO permission (action, target_id)
SELECT specific_access_policy, entity_id
FROM entity_payload
WHERE specific_access_policy is not null;

ALTER TABLE entity_payload
    DROP COLUMN specific_access_policy;

UPDATE permission
SET created_at = '1970-01-01 00:00:00.033000'
WHERE created_at is null;

UPDATE permission
SET modified_at = '1970-01-01 00:00:00.033000'
WHERE modified_at is null;

UPDATE permission
set id = concat('urn:ngsi-ld:Permission:', gen_random_uuid())
WHERE id is null;

UPDATE permission
SET action = 'read'
WHERE action = 'canRead'
   OR action = 'AUTH_READ';

UPDATE permission
SET action = 'write'
WHERE action = 'canWrite'
   OR action = 'AUTH_WRITE';

UPDATE permission
SET action = 'admin'
WHERE action = 'canAdmin';

UPDATE permission
SET action = 'own'
WHERE action = 'isOwner';
