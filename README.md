# Quick start

* Clone the repository

```
git clone git@bitbucket.org:eglobalmark/context-registry.git
```

* Start Kafka and Neo4j with the provided `docker-compose.yml` file (edit the `.env` file if you need to specify a different host for Kafka) :

```
docker-compose up -d && docker-compose logs -f
```

* Create a `src/main/resources/application-dev.properties` file and put in it the configuration properties you want to override

* Start the application :

```
./gradlew bootRun
```

* Important create namespaces on neo4j
```
CREATE (:NamespacePrefixDefinition {
  `https://diatomic.eglobalmark.com/ontology#`: 'diat',
  `http://xmlns.com/foaf/0.1/`: 'foaf',
  `https://uri.etsi.org/ngsi-ld/v1/ontology#`: 'ngsild'})

CREATE INDEX ON :Resource(uri)
```

# Sample queries

* Create a new entity

```
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/data/observation_sensor.jsonld
```

* Get an entity by type

```
http GET http://localhost:8080/ngsi-ld/v1/entities?type=BeeHive Content-Type:application/json
```

* Insert jsonld
```
./insertData.sh
```
* return entities list by label
```
curl -vX GET http://localhost:8080/ngsi-ld/v1/entities?type=diat__BeeHive
```
* return related objects by label
```
curl -vX GET http://localhost:8080/ngsi-ld/v1/entities?type=diat__BeeHive
```
* return object by URI
```
curl -vX GET http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Beekeeper:TEST1
```
* return object by QUERY (is a cypher query) : find all the objects that connects to Beekeeper
```
curl -g -X GET http://localhost:8080/ngsi-ld/v1/entities?type=diat__BeeHive&query=ngsild__connectsTo==urn:ngsi-ld:Beekeeper:TEST1
curl -g -X GET http://localhost:8080/ngsi-ld/v1/entities?type=diat__Beekeeper&query=foaf__name==TEST1

```
* get by uri using rdf/describe
```
curl -X GET http://neo4j:test@docker:7474/rdf/describe/uri/urn:ngsi-ld:SmartDoor:0021?format=JSON-LD
http://docker:7474/rdf/describe/find/diat__BeeHive/foaf__name/ParisBeehive12?format=JSON-LD

** Get entity related objects
* give me all the BeeHive (search by label)
curl -g -X POST http://neo4j:test@docker:7474/rdf/cypheronrdf -H "Content-Type: application/json" -d '{ "cypher" : "MATCH (o:diat__BeeHive )-[r]->(s)  RETURN o, r" , "format": "JSON-LD" }'
* give me all the BeeHive (search by label/property/value)
curl -g -X POST http://neo4j:test@docker:7474/rdf/cypheronrdf -H "Content-Type: application/json" -d '{ "cypher" : "MATCH (o:diat__BeeHive { foaf__name : \"ParisBeehive12\" })-[r]->(s)  RETURN o, r" , "format": "JSON-LD" }'
* give me all the Doors attached to a SmartDoor (search by label/relation/uri)
curl -g -X POST http://neo4j:test@docker:7474/rdf/cypheronrdf -H "Content-Type: application/json" -d '{ "cypher" : "MATCH (o:diat__Door )-[r:ngsild__connectsTo]->(s { uri : \"urn:ngsi-ld:SmartDoor:0021\" })  RETURN o, r" , "format": "JSON-LD" }'
```
* Useful queries
```
MATCH (n:diat__Beekeeper) RETURN n.uri
```
Delete all
```
MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r
```
Relations
```
MATCH (s:Resource)-[:ngsild__connectsTo]-(o:Resource) RETURN s,o
```

MATCH ()-[r:ngsild__connectsTo]-(n:diat__Beekeeper{foaf__name:'TEST1'} ) RETURN n
MATCH (s:diat__Beekeeper{foaf__name: 'TEST1'})-[r:ngsild__connectsTo]-(o ) RETURN s