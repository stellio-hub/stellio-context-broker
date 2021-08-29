MATCH (r:Attribute:Relationship)-[]->(e:Entity)
SET r.objectId = e.id
