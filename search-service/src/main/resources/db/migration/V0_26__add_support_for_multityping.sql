alter table temporal_entity_attribute
add column types text[];

update temporal_entity_attribute
set types = array[type];

alter table temporal_entity_attribute
drop column type;
