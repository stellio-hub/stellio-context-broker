CREATE TABLE subject_referential(
    subject_id              UUID NOT NULL PRIMARY KEY,
    subject_type            VARCHAR(64) NOT NULL,
    global_roles            TEXT[],
    groups_memberships      TEXT[]
);

CREATE TABLE entity_access_rights(
    subject_id              UUID NOT NULL,
    access_right            VARCHAR(64) NOT NULL,
    entity_id               VARCHAR(255) NOT NULL
);

ALTER TABLE entity_access_rights
    ADD CONSTRAINT entity_access_rights_uniqueness UNIQUE (subject_id, access_right, entity_id);
