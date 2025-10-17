UPDATE permission
SET assignee = 'urn:ngsi-ld:Subject:authenticated'
WHERE assignee is null;

alter table permission
    alter column assignee set not null;