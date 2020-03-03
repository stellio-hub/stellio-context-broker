CREATE TABLE entity_temporal_property(
    entity_id           VARCHAR(64) NOT NULL,
    type                VARCHAR(64) NOT NULL,
    attribute_name      VARCHAR(64) NOT NULL,
    observed_by         VARCHAR(64) NOT NULL,
    CONSTRAINT entity_pkey PRIMARY KEY(entity_id, attribute_name)
);
