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
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/beehive.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/beehive_nested.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/beekeeper.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/door.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/observation_door.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/observation_sensor.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/sensor.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/smartdoor.jsonld --header "Content-Type: application/ld+json"
```
* return entities list by label
```
curl -vX GET http://localhost:8080/ngsi-ld/v1/entities?type=diat__BeeHive
```
* return related objects by label
```
curl -vX GET http://localhost:8080/ngsi-ld/v1/entities/graph?type=diat__BeeHive
```
* return object by URI
```
curl -vX GET http://localhost:8080/ngsi-ld/v1/entities/uri?uri=urn:ngsi-ld:Beekeeper:TEST1
```

* Important create namespaces on neo4j
```
CREATE (:NamespacePrefixDefinition {
  `https://diatomic.eglobalmark.com/ontology#`: 'diat',
  `http://xmlns.com/foaf/0.1/`: 'foaf',
  `https://uri.etsi.org/ngsi-ld/v1/ontology#`: 'ngsild'})

CREATE INDEX ON :Resource(uri)
```

* Useful queries
```
MATCH (n:ns0__Beekeeper) RETURN n.uri
```
Delete all
```
MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r
```

