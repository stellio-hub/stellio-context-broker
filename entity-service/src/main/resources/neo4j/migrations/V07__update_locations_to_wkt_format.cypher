MATCH (entity:Entity)
WHERE entity.location IS NOT NULL
SET entity.location = "POINT (" + entity.location.x + " " + entity.location.y + ")"
