# Quick start

* Start Kafka and Neo4j with the provided `docker-compose.yml` file (edit the `.env` file if you need to specify a different host for Kafka) :

```
docker-compose up -d && docker-compose logs -f
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
