CREATE TABLE subscription(
    id                  VARCHAR(64) PRIMARY KEY,
    type                VARCHAR(12) NOT NULL,
    name                VARCHAR(256),
    description         VARCHAR(1024),
    notif_attributes    VARCHAR(1024),
    notif_format        VARCHAR(12),
    endpoint_uri        VARCHAR(256) NOT NULL,
    endpoint_accept     VARCHAR(24),
    status              VARCHAR(12),
    times_sent          INTEGER NOT NULL,
    last_notification   TIMESTAMPTZ,
    last_failure        TIMESTAMPTZ,
    last_success        TIMESTAMPTZ
);

CREATE TABLE entity_info(
    id                  VARCHAR(255),
    id_pattern          VARCHAR(255),
    type                VARCHAR(64),
    subscription_id     VARCHAR(64)
);
