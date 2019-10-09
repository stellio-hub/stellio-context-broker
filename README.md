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
docker run easyglobalmarket/context-registry:latest
```

# Sample queries

## API queries

* Create a new entity

```
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/data/beehive.jsonld
```

* Get an entity by type

```
http GET http://localhost:8080/ngsi-ld/v1/entities?type=diat__BeeHive Content-Type:application/json
```

* Bootstrap some data in a batch

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
curl -g -X GET http://localhost:8080/ngsi-ld/v1/entities?type=diat__BeeHive&q=ngsild__connectsTo==urn:ngsi-ld:Beekeeper:TEST1
curl -g -X GET http://localhost:8080/ngsi-ld/v1/entities?type=diat__Beekeeper&q=foaf__name==TEST1
```

## Cypher queries

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
MATCH (s:Resource)-[:ngsild__connectsTo]-(o:Resource) RETURN s,o
MATCH ()-[r:ngsild__connectsTo]-(n:diat__Beekeeper{foaf__name:'TEST1'} ) RETURN n
MATCH (s:diat__Beekeeper{foaf__name: 'TEST1'})-[r:ngsild__connectsTo]-(o ) RETURN s
```
