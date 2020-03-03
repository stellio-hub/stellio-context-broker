#!/bin/sh
echo "START injecting jsonld..."

echo "START injecting apic data..."

echo "smartdoor START......"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/smartDoor.json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
echo "smartdoor END......"

echo "sensors START......"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/Sensor01XYZ\(temperature\).json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/Sensor002XIO52\(outgoing\).json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/Sensor0021\(incoming\).json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/Sensor1541jdh\(humidity\).json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/Sensor016352\(batteryLevel\).json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
echo "sensors END......"

echo "apiary START......"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/apiary.json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
echo "apiary END......"

echo "beekeeper START......"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/beekeeper.json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
echo "beekeeper END......"

echo "beehive START......"
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/ngsild/apic/beehive.json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
echo "beehive END......"

echo "Linking smartdoor to beehive START......"
http POST http://localhost:8082/ngsi-ld/v1/entities/urn:ngsi-ld:SmartDoor:00YHG/attrs Content-Type:application/json < src/test/resources/ngsild/apic/smartDoor_update.json Link:"<https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/apic.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
echo "Linking smartdoor to beehive END......"

echo "END injecting apic data..."

echo "END injection"
