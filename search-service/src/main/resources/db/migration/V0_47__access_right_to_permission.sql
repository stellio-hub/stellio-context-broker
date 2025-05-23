ALTER TABLE entity_access_rights
    RENAME TO permission;

ALTER TABLE permission
    RENAME COLUMN subject_id TO assignee;

alter table permission
    alter column assignee drop not null;

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

-- guess the permission assigner with the entity creator of the target
UPDATE permission
SET assigner = owner_permission.assignee
FROM permission as owner_permission
WHERE permission.target_id = owner_permission.target_id
  AND owner_permission.action = 'own';


-- if there is still a null assigner replace it with the stellio_team client
UPDATE permission
SET assigner = subject_referential.subject_id
FROM subject_referential
WHERE permission.assigner is null
  AND subject_type = 'CLIENT'
  AND subject_info -> 'value' ->> 'clientId' = 'stellio-team'