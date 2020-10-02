MATCH (n:Relationship)-[rel]->(e:Entity)
WITH rel, [word IN split(type(rel), '_') | toLower(word)] as list
WITH rel, reduce(accumulator = '', word IN list | accumulator + replace(word,left(word,1),toUpper(left(word,1)))) as res
WITH rel, toLower(substring(res, 0, 1)) + substring(res, 1) as result
call apoc.refactor.setType(rel, result)
YIELD input, output
RETURN input, output