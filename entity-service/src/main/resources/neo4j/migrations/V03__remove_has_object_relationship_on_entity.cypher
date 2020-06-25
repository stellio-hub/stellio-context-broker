MATCH (r:Relationship)-[rel:HAS_OBJECT]->(e:Entity)
DELETE rel;
