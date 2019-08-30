#!/bin/sh
echo "START injecting jsonld..."
echo "beekeeper START......"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/beekeeper.jsonld --header "Content-Type: application/ld+json"
echo "beekeeper END......"
echo "beehive START......"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/beehive.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/beehive_not_connected.jsonld --header "Content-Type: application/ld+json"
echo "beehive END......"
echo "door START......"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/door.jsonld --header "Content-Type: application/ld+json"
echo "door END......"
echo "obs START......"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/observation_door.jsonld --header "Content-Type: application/ld+json"
echo "obs END......"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/observation_sensor.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/sensor.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/data/smartdoor.jsonld --header "Content-Type: application/ld+json"
echo "END injection"
