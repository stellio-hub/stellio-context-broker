ALTER TABLE context_source_registration
    ADD COLUMN IF NOT EXISTS tenant text;
