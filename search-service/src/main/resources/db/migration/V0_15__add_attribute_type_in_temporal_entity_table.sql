alter table temporal_entity_attribute
    add column attribute_type varchar(32);

-- existing temporal attributes are properties only, so we can safely init them with this value
update temporal_entity_attribute
    set attribute_type = 'Property';
