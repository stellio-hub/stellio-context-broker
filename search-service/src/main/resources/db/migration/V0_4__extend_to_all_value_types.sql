ALTER TABLE entity_temporal_property
    ADD COLUMN attribute_value_type VARCHAR(32) NOT NULL DEFAULT 'MEASURE';

CREATE TABLE property_observation(
     observed_at    TIMESTAMPTZ         NOT NULL,
     value          DOUBLE PRECISION    NOT NULL,
     attribute_name VARCHAR(255)        NOT NULL,
     UNIQUE (observed_at, attribute_name)
)
