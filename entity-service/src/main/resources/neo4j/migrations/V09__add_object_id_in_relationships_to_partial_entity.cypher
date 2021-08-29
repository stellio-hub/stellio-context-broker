MATCH (r:Attribute:Relationship)-[]->(e:PartialEntity)
SET r.objectId = e.id
