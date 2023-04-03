-- Default indexes created by Timescale that should not have been removed
-- https://docs.timescale.com/use-timescale/latest/hypertables/about-hypertables/#hypertable-indexes
CREATE INDEX IF NOT EXISTS attribute_instance_time_idx
    ON attribute_instance(time DESC );
CREATE INDEX IF NOT EXISTS attribute_instance_audit_time_idx
    ON attribute_instance_audit(time DESC);

-- Indexes on main keys used when doing a temporal query
-- As recommended in https://docs.timescale.com/getting-started/latest/create-hypertable/#creating-your-first-hypertable
CREATE INDEX attribute_instance_tea_idx
    ON attribute_instance(temporal_entity_attribute, time DESC);
CREATE INDEX attribute_instance_audit_tea_timeproperty_idx
    ON attribute_instance_audit(temporal_entity_attribute, time_property, time DESC);

-- Remove current unwanted indexes
DROP INDEX IF EXISTS attribute_instance_audit_tea_time_property_idx;
DROP INDEX IF EXISTS attribute_instance_temporal_entity_attribute_idx;

-- Same as attribute_instance_time_idx created above, but with a deprecated reference to observed_at column
DROP INDEX IF EXISTS attribute_instance_observed_at_idx;
