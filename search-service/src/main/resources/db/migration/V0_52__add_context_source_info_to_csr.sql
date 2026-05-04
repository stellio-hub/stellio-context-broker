ALTER TABLE context_source_registration
    ADD COLUMN IF NOT EXISTS context_source_info jsonb;
