# Quick start

* Clone the repository

```
git clone git@bitbucket.org:eglobalmark/context-registry.git
```

* Start Kafka and Neo4j with the provided `docker-compose.yml` file (edit the `.env` file if you need to specify a different host for Kafka) :

```
docker-compose up -d && docker-compose logs -f
```

* Update your `/etc/hosts` file and add the following entry :

```
127.0.0.1       dh-local-docker
```

* Bootstrap some data in a batch (optional)

```
./insertData.sh
```

* Create a `src/main/resources/application-dev.properties` file and put in it the configuration properties you want to override

* Start the application :

```
./gradlew bootRun
```

* Connect to the Neo4j browser and create the namespaces :

```
CREATE (:NamespacePrefixDefinition {
  `https://diatomic.eglobalmark.com/ontology#`: 'diat',
  `http://xmlns.com/foaf/0.1/`: 'foaf',
  `http://example.org/ngsi-ld/`: 'example',
  `https://uri.etsi.org/ngsi-ld/v1/ontology#`: 'ngsild'})

CREATE INDEX ON :Resource(uri)
CREATE CONSTRAINT ON (camera:Camera) ASSERT camera:uri IS UNIQUE
CREATE CONSTRAINT ON (asN:availableSpotNumber) ASSERT asN:uri IS UNIQUE
CREATE CONSTRAINT ON (offStreetParking:OffStreetParking) ASSERT offStreetParking:uri IS UNIQUE
CREATE CONSTRAINT ON (vehicle:Vehicle) ASSERT vehicle:uri IS UNIQUE
CREATE CONSTRAINT ON (person:Person) ASSERT camera:uri IS UNIQUE

```

* Create and publish a Docker image:

```
./gradlew jib
```

# Work locally with Docker images

* Build a tar image:

```
./gradlew jibBuildTar
```

* Load the tar image into Docker:

```
docker load --input build/jib-image.tar
```

* Run the image:

```
docker run easyglobalmarket/stellio-entity-service:latest
```

# Sample queries

## API queries

* Create a new entity

```
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/beehive.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Eventually, create a second one

```
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/beehive_2.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Get entities by type

```
http http://localhost:8082/ngsi-ld/v1/entities  type==BeeHive Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Get an entity by URI

```
http http://localhost:8082/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC  Content-Type:application/json

```

* Get entities by relationships

```
http http://localhost:8082/ngsi-ld/v1/entities  type==BeeHive  q==connectsTo==urn:ngsi-ld:Beekeeper:Pascal Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
http http://localhost:8082/ngsi-ld/v1/entities  type==Vehicle  q==isParked==urn:ngsi-ld:OffStreetParking:Downtown1 Link:"<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json

```

* Get entities by property

```
http http://localhost:8082/ngsi-ld/v1/entities  type==BeeHive  q==name==ParisBeehive12 Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Update the property of an entity

```
http PATCH http://localhost:8082/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC/attrs/name Content-Type:application/json < src/resources/ngsild/sensor_update_attribute.json Link:"<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
```

* Update an entity

```
http PATCH http://localhost:8082/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC/attrs  Content-Type:application/json < src/resources/ngsild/sensor_update.json Link:"<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
```

## Cypher queries

to access the Neo4j interface navigate to $DOCKERHOST:7474

* Get all URIs for beekeepers

```
MATCH (n:diat__Beekeeper) RETURN n.uri
```

* Delete all entities in the DB

```
MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r
```

* Let's play with relations

```
MATCH (s:Resource)-[:connectsTo]-(o:Resource) RETURN s,o
MATCH ()-[r:connectsTo]-(n:Beekeeper{name:'TEST1'} ) RETURN n
MATCH (s:Beekeeper{name: 'TEST1'})-[r:connectsTo]-(o ) RETURN s
```
