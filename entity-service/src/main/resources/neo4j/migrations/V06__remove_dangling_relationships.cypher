MATCH (rel:Relationship)
WHERE NOT (rel)-[]->(:Entity) AND NOT (rel)-[]->(:PartialEntity)
DELETE rel
