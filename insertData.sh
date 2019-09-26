#!/bin/sh
echo "START injecting jsonld..."
echo "beekeeper START......"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/beekeeper.json --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/beekeeper_notconnected.json --header "Content-Type: application/ld+json"
echo "beekeeper END......"
echo "beehive START......"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/beehive.json --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/beehive_notconnected.json --header "Content-Type: application/ld+json"
echo "beehive END......"
echo "door START......"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/door.json --header "Content-Type: application/ld+json"
echo "door END......"
echo "obs START......"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/observation_door.json --header "Content-Type: application/ld+json"
echo "obs END......"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/observation_sensor.json --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/sensor.json --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/smartdoor.json --header "Content-Type: application/ld+json"

curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/parking_ngsild.json --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities -d @src/test/resources/ngsild/vehicle_ngsild.json --header "Content-Type: application/ld+json"

echo "END injection"
