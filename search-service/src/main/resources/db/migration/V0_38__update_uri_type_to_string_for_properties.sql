update temporal_entity_attribute
    set attribute_value_type = 'STRING'
    where attribute_value_type = 'URI'
    and attribute_type = 'Property';
