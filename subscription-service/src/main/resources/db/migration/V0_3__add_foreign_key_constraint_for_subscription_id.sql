ALTER TABLE entity_info
ADD CONSTRAINT FK_SubscriptionId
FOREIGN KEY (subscription_id) REFERENCES subscription(id)
on delete cascade;

ALTER TABLE geometry_query
ADD CONSTRAINT FK_SubscriptionId
FOREIGN KEY (subscription_id) REFERENCES subscription(id)
on delete cascade;
