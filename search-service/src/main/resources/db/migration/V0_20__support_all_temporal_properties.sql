alter table attribute_instance add column time_property VARCHAR(20);
update attribute_instance set time_property = 'OBSERVED_AT';
alter table attribute_instance rename column observed_at to time;

ALTER TABLE attribute_instance DROP CONSTRAINT instance_unicity;
ALTER TABLE attribute_instance ADD CONSTRAINT instance_unicity UNIQUE (temporal_entity_attribute, time, time_property);
