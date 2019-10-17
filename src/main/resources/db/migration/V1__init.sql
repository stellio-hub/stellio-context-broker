CREATE TABLE measure (
  time           TIMESTAMPTZ       NOT NULL,
  container      TEXT              NOT NULL,
  content_info   TEXT              NOT NULL,
  unit           TEXT              NOT NULL,
  content        DOUBLE PRECISION  NOT NULL
);

SELECT create_hypertable('measure', 'time');
-- see when / if it's needed
-- SELECT create_hypertable('measure', 'time', chunk_time_interval => interval '1 day');
