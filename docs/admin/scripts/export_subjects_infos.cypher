WITH "MATCH (n:`https://ontology.eglobalmark.com/authorization#Client`)-[:HAS_VALUE]->(pSid:Property {name: 'https://ontology.eglobalmark.com/authorization#serviceAccountId'})
MATCH (n)-[:HAS_VALUE]->(pClientId:Property {name: 'https://ontology.eglobalmark.com/authorization#clientId'})
RETURN substring(n.id, 19), substring(pSid.value, 17), pClientId.value" AS query
CALL apoc.export.csv.query(query, "/subject_export/export_clients.csv", {})
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data;

WITH "MATCH (n:`https://ontology.eglobalmark.com/authorization#User`)-[:HAS_VALUE]->(pUsername:Property {name: 'https://ontology.eglobalmark.com/authorization#username'})
OPTIONAL MATCH (n)-[:HAS_VALUE]->(pGivenName:Property {name: 'https://ontology.eglobalmark.com/authorization#givenName'})
OPTIONAL MATCH (n)-[:HAS_VALUE]->(pFamilyName:Property {name: 'https://ontology.eglobalmark.com/authorization#familyName'})
RETURN substring(n.id, 17), pUsername.value, pGivenName.value, pFamilyName.value" AS query
CALL apoc.export.csv.query(query, "/subject_export/export_users.csv", {})
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data;

WITH "MATCH (n:`https://ontology.eglobalmark.com/authorization#Group`)-[:HAS_VALUE]->(pName:Property {name: 'https://schema.org/name'})
RETURN substring(n.id, 18), pName.value" AS query
CALL apoc.export.csv.query(query, "/subject_export/export_groups.csv", {})
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data;
