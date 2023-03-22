ALTER TABLE entity_info RENAME TO entity_selector;

ALTER TABLE entity_selector ALTER COLUMN "type" TYPE text;
