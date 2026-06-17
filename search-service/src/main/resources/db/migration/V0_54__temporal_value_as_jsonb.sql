-- change value type to jsonb in multiple steps to support compressed table

-- attribute_instance
ALTER TABLE attribute_instance
    ADD COLUMN json_value JSONB;

UPDATE attribute_instance
SET json_value = to_jsonb(value);

ALTER TABLE attribute_instance
    DROP COLUMN value;

ALTER TABLE attribute_instance
    RENAME COLUMN json_value TO value;

-- attribute_instance_audit
ALTER TABLE attribute_instance_audit
    ADD COLUMN json_value JSONB;

UPDATE attribute_instance_audit
SET json_value = to_jsonb(value);

ALTER TABLE attribute_instance_audit
    DROP COLUMN value;

ALTER TABLE attribute_instance_audit
    RENAME COLUMN json_value TO value;


-- field who already had a json value
UPDATE attribute_instance
SET value = (value #>> '{}')::jsonb
WHERE payload #>> '{@type, 0}' in (
    'https://uri.etsi.org/ngsi-ld/LanguageProperty',
    'https://uri.etsi.org/ngsi-ld/JsonProperty',
    'https://uri.etsi.org/ngsi-ld/VocabProperty'
)
AND LEFT(value #>> '{}', 1) in ('{', '['); -- avoid transforming 'urn:ngsi-ld:null' or other non json

UPDATE attribute_instance_audit
SET value = (value #>> '{}')::jsonb
WHERE payload #>> '{@type, 0}' in (
    'https://uri.etsi.org/ngsi-ld/LanguageProperty',
    'https://uri.etsi.org/ngsi-ld/JsonProperty',
    'https://uri.etsi.org/ngsi-ld/VocabProperty'
)
AND LEFT(value #>> '{}', 1) in ('{', '[');