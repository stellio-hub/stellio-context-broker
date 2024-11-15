CREATE TABLE context_source_registration
(
    id text,
    endpoint text NOT NULL,
    mode text NOT NULL,
	information jsonb NOT NULL,
	operations text[] NOT NULL,
	registration_name text,
    observation_interval_start timestamp with time zone,
    observation_interval_end timestamp with time zone,
    management_interval_start timestamp with time zone,
    management_interval_end timestamp with time zone,
	sub text,
    created_at timestamp with time zone NOT NULL,
    modified_at timestamp with time zone,
    status text,
    times_sent integer,
    times_failed integer,
    last_success timestamp with time zone,
    last_failure timestamp with time zone,

    CONSTRAINT context_source_registration_pkey PRIMARY KEY (id)
)