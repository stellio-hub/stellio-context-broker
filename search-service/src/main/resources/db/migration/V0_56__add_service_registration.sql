CREATE TABLE service_registration
(
    id text,
    endpoint text NOT NULL,
    mode text,
    entities jsonb NOT NULL,
    service_information jsonb NOT NULL,
    q text,
    geo_q jsonb,
    scope_q text,
    sub text,
    created_at timestamp with time zone NOT NULL,
    modified_at timestamp with time zone NOT NULL,

    CONSTRAINT service_registration_pkey PRIMARY KEY (id)
);
