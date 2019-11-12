CREATE TABLE observation (
  observed_by   TEXT                NOT NULL,
  observed_at   TIMESTAMPTZ         NOT NULL,
  value         DOUBLE PRECISION    NOT NULL,
  unit_code     TEXT                NOT NULL,
  latitude      DOUBLE PRECISION            ,
  longitude     DOUBLE PRECISION            ,
  UNIQUE (observed_by, observed_at, value, unit_code)
);

SELECT create_hypertable('measure', 'observed_at', 'observed_by');
-- see when / if it's needed
-- SELECT create_hypertable('measure', 'time', chunk_time_interval => interval '1 day');
