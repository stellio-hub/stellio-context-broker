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
INSERT INTO permission (id, action, target_id)
SELECT specific_access_policy, entity_id
FROM entity_payload
WHERE specific_access_policy is not null;

ALTER TABLE entity_payload
    DROP COLUMN specific_access_policy;

-- guess the permission creation date with the entity it target
UPDATE permission
SET created_at  = entity_payload.created_at,
    modified_at = entity_payload.created_at
FROM entity_payload
WHERE permission.target_id = entity_payload.entity_id;

UPDATE permission
set id = concat("urn:ngsi-ld:Permission:", gen_random_uuid());

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
