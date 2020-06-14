CREATE INDEX entity_id_index FOR (e:Entity) ON (e.id);
CREATE INDEX attribute_id_index FOR (a:Attribute) ON (a.id);
CREATE INDEX property_name_index FOR (p:Property) ON (p.name);
