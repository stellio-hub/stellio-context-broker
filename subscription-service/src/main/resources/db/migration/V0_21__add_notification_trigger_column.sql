alter table subscription
    add column notification_trigger text[];

update subscription
    set notification_trigger = '{ "attributeCreated", "attributeUpdated" }';
