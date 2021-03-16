MATCH (rel:Relationship)
WHERE NOT (rel)-[]->(:Entity)
DELETE rel
