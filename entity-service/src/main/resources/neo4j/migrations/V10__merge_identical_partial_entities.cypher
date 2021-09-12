// merge any duplicated partial entity before adding the index
MATCH (n:PartialEntity)
WITH n.id as id, COLLECT(n) AS ns WHERE size(ns) > 1
CALL apoc.refactor.mergeNodes(ns) YIELD node
RETURN node;
