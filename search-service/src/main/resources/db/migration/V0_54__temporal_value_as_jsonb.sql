ALTER TABLE attribute_instance
    ALTER COLUMN value TYPE JSONB
        USING to_jsonb(value);

ALTER TABLE attribute_instance_audit
    ALTER COLUMN value TYPE JSONB
        USING to_jsonb(value);


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