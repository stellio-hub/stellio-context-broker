ALTER TABLE attribute_instance
    ALTER COLUMN value TYPE JSONB
        USING value::jsonb;

ALTER TABLE attribute_instance_audit
    ALTER COLUMN value TYPE JSONB
        USING value::jsonb;

UPDATE attribute_instance
SET value = (value::text::jsonb #>> '{}')::jsonb
WHERE payload #>> '{@type, 0}' in (
    'https://uri.etsi.org/ngsi-ld/LanguageProperty',
    'https://uri.etsi.org/ngsi-ld/JsonProperty',
    'https://uri.etsi.org/ngsi-ld/VocabProperty'
);