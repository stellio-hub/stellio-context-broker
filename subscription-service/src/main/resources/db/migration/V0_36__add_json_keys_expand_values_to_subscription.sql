ALTER TABLE subscription ADD COLUMN IF NOT EXISTS json_keys text[];
ALTER TABLE subscription ADD COLUMN IF NOT EXISTS expand_values text[];
