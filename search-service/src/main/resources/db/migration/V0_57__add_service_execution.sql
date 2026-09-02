CREATE TABLE service_execution
(
    id text,
    service_id text NOT NULL,
    entity_id text NOT NULL,
    entity_type text NOT NULL,
    input jsonb NOT NULL,
    service_name text,
    execution_status text NOT NULL,
    progress double precision,
    output jsonb,
    response_status_code integer,
    sub text,
    created_at timestamp with time zone NOT NULL,
    modified_at timestamp with time zone NOT NULL,

    CONSTRAINT service_execution_pkey PRIMARY KEY (id)
);
