alter table attribute_instance add column time_property VARCHAR(20);
update attribute_instance set time_property = "observed_at";
alter table attribute_instance rename column observed_at to time;
