#!/bin/sh
echo "START injecting jsonld..."
echo "beekeeper START......"
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/beekeeper.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/beekeeper_notconnected.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
echo "beekeeper END......"
echo "beehive START......"

http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/beehive.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/beehive_notconnected.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"

echo "beehive END......"
echo "door START......"

http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/door.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"

echo "door END......"
echo "obs START......"

http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/observation_door.json Link:"<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"

echo "obs END......"

http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/observation_sensor.json Link:"<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/sensor.json Link:"<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/smartdoor.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"

http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/parking_ngsild.json Link:"<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#example; type=application/ld+json"
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/vehicle_ngsild.json Link:"<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#example; type=application/ld+json"


echo "END injection"


