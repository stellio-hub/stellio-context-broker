alter table entity_info
    alter column id type text,
    alter column id_pattern type text,
    alter column type type text,
    alter column subscription_id type text;

alter table geometry_query
    alter column georel type text,
    alter column geometry type text,
    alter column coordinates type text,
    alter column geoproperty type text,
    alter column subscription_id type text;

alter table subscription
    alter column id type text,
    alter column type type text,
    alter column subscription_name type text,
    alter column description type text,
    alter column notif_attributes type text,
    alter column notif_format type text,
    alter column endpoint_uri type text,
    alter column endpoint_accept type text,
    alter column status type text,
    alter column q type text,
    alter column sub type text,
    alter column watched_attributes type text;
