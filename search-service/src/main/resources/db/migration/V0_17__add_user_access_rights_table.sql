CREATE TABLE subject_access_rights(
    subject_id              VARCHAR(64) NOT NULL PRIMARY KEY,
    subject_type            VARCHAR(64) NOT NULL,
    global_role             VARCHAR(64),
    allowed_read_entities   TEXT[],
    allowed_write_entities  TEXT[]
)
