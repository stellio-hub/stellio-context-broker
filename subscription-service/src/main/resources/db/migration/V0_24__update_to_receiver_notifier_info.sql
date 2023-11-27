alter table subscription
    rename column endpoint_info to endpoint_receiver_info;

alter table subscription
    add column endpoint_notifier_info jsonb;
