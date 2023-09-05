CREATE TABLE scope_history(
    entity_id       text not null
        references entity_payload(entity_id) on delete cascade,
    value           jsonb not null,
    time            timestamp with time zone not null,
    time_property   text not null,
    sub             text
);

SELECT create_hypertable('scope_history','time', chunk_time_interval => INTERVAL '4 weeks');
