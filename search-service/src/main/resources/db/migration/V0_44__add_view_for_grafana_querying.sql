CREATE VIEW flattened_entities_attributes_values AS (
    SELECT entity_payload.entity_id,
       TRIM(JSONB_PATH_QUERY_FIRST(entity_payload.payload, '$."https://schema.org/name"[0]."https://uri.etsi.org/ngsi-ld/hasValue"[0]."@value"')::TEXT, '"') AS "entity_name",
       attribute_name,
       dataset_id,
       time,
       measured_value AS value
    FROM attribute_instance
    LEFT JOIN temporal_entity_attribute ON temporal_entity_attribute.id = attribute_instance.temporal_entity_attribute
    LEFT JOIN entity_payload ON entity_payload.entity_id = temporal_entity_attribute.entity_id
);
