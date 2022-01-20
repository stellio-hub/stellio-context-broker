-- Ensure all dates on relationships nodes are in the correct format
MATCH (r:Relationship) SET r.createdAt = datetime(r.createdAt) SET r.modifiedAt = datetime(r.modifiedAt);
-- Ensure there are no duplicate entities in the graph
MATCH (e:Entity) WITH e.id AS id, COLLECT(e) AS nodelist, COUNT(*) AS count WHERE count > 1 CALL apoc.refactor.mergeNodes(nodelist) YIELD node RETURN node;
-- Ensure all relationships have the objectId property correctly filled
MATCH (r:Attribute:Relationship)-[]->(e:Entity) SET r.objectId = e.id;
