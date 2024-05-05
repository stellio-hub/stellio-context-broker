-- rename exiting authz rights
UPDATE entity_access_rights
SET access_right =
    CASE
        WHEN access_right = 'rCanAdmin' THEN 'canAdmin'
        WHEN access_right = 'rCanWrite' THEN 'canWrite'
        WHEN access_right = 'rCanReadm' THEN 'canRead'
    END;

WITH entities AS (
    SELECT entity_id, count(*) as admin_right_count
    FROM entity_access_rights
    WHERE access_right = 'canAdmin'
    GROUP BY entity_id
)
UPDATE entity_access_rights
SET access_right = 'isOwner'
WHERE entity_id IN (select entity_id from entities where admin_right_count = 1)
AND access_right = 'canAdmin';

-- set isOwner for entities with more than admin right
WITH entities AS (
    SELECT entity_id, count(*) as admin_right_count
    FROM entity_access_rights
    WHERE access_right = 'canAdmin'
    GROUP BY entity_id
), entities_more_than_one_admin AS (
    SELECT entity_id
    FROM entities
    WHERE admin_right_count > 1
), entities_with_oldest_date AS (
    SELECT entity_id, min(created_at) as created_at
    FROM temporal_entity_attribute
    WHERE entity_id IN (select entity_id from entities_more_than_one_admin)
    GROUP BY entity_id
), entities_oldest_with_sub AS (
    select distinct tea.entity_id, sub
    from temporal_entity_attribute tea, entities_with_oldest_date
    inner join lateral (
        select sub
        from attribute_instance_audit
        where temporal_entity_attribute = tea.id
          and time_property = 'CREATED_AT'
          and sub is not null
    ) l on true
    where tea.entity_id = entities_with_oldest_date.entity_id
    and tea.created_at = entities_with_oldest_date.created_at
)
update entity_access_rights
set access_right = 'isOwner',
    subject_id = entities_oldest_with_sub.sub
from entities_oldest_with_sub
where entity_access_rights.entity_id = entities_oldest_with_sub.entity_id
and entity_access_rights.access_right = 'canAdmin';
