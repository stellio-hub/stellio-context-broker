CREATE TABLE observation (
  observed_by   TEXT                NOT NULL,
  observed_at   TIMESTAMPTZ         NOT NULL,
  value         DOUBLE PRECISION    NOT NULL,
  unit_code     TEXT                NOT NULL,
  latitude      DOUBLE PRECISION    NOT NULL,
  longitude     DOUBLE PRECISION    NOT NULL
);

SELECT create_hypertable('measure', 'observed_at');
-- see when / if it's needed
-- SELECT create_hypertable('measure', 'time', chunk_time_interval => interval '1 day');
