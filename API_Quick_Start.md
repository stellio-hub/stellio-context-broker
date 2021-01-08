# Quickstart

This quickstart guide shows a real use case scenario of interaction with the API in an Apiculture context.

## Prepare your environment

The provided examples make use of the HTTPie command line tool (installation instructions: https://httpie.org/docs#installation)

All requests are grouped in a Postman collection that can be found [here](samples/API_Quick_Start.postman_collection.json). 
For more details about how to import a Postman collection see https://learning.postman.com/docs/getting-started/importing-and-exporting-data/.    

Start a Stellio instance. You can use the provided Docker compose configuration in this directory:

```shell
docker-compose -f docker-compose.yml up -d && docker-compose -f docker-compose.yml logs -f
```

Export the link to the JSON-LD context used in this use case in an environment variable for easier referencing in
the requests:

````shell
export CONTEXT_LINK="<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
````

## Case study

This case study is written for anyone who wants to get familiar with the API, we use a real example to make it more concrete.

We will create the following entities:
- A beekeeper
- An apiary
- A beehive
- Sensors linked to the beehive which measures two metrics (temperature and humidity)

## Queries

* We start by creating the beekeeper, the apiary, the beehive and sensors 

```shell
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/beekeeper.jsonld
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/apiary.jsonld
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/sensor_temperature.jsonld
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/sensor_humidity.jsonld
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/beehive.jsonld
```

* We can delete the created entities and recreate them in batch (optional):
   
```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Beekeeper:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Apiary:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:02
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01

http POST http://localhost:8080/ngsi-ld/v1/entityOperations/create Content-Type:application/ld+json < samples/apiculture_entities.jsonld
```

* The created beehive can be retrieved by id:

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01 Link:$CONTEXT_LINK
```

* Or by querying all entities of type BeeHive:

```shell
http http://localhost:8080/ngsi-ld/v1/entities type==BeeHive Link:$CONTEXT_LINK
```

* Let's add a name to the created beehive: 

```shell
http POST http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:$CONTEXT_LINK < samples/beehive_addName.json
```

* The recently added name property can be deleted: 

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/name Link:$CONTEXT_LINK
```

* Let's create a subscription to the beehive that sends a notification when the temperature exceeds 40 

```shell
http POST http://localhost:8080/ngsi-ld/v1/subscriptions Content-Type:application/ld+json < samples/subscription_to_beehive.jsonld
```

* The created subscription can be retrieved by id

```shell
http http://localhost:8080/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Link:$CONTEXT_LINK
```

* By doing this, increasing the beehive temperature to 42 will raise a notification 
  (the notification is sent via a POST request to the provided URI when creating the subscription, 
  please consider providing working `endpoint` params in order to receive it)

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:$CONTEXT_LINK < samples/beehive_updateTemperature.json
```

* We can also update the beehive humidity 

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:$CONTEXT_LINK < samples/beehive_updateHumidity.json
```

* Since we updated both temperature and humidity, we can get the temporal evolution of those properties

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 \
    timerel==between \
    time==2019-10-25T12:00:00Z \
    endTime==2020-10-27T12:00:00Z \
    Link:$CONTEXT_LINK
```

Sample payload returned showing the temporal evolution of temperature and humidity properties:

```json
{
    "@context": [
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld"
    ],
    "humidity": [
        {
            "instanceId": "urn:ngsi-ld:Instance:a768ffb8-79e0-488f-9a4c-e7217ba2dff4",
            "observedAt": "2019-10-26T21:35:52.986010Z",
            "type": "Property",
            "value": 58.0
        },
        {
            "instanceId": "urn:ngsi-ld:Instance:28e44b9e-86f5-4bbb-a363-718d849d1782",
            "observedAt": "2019-10-26T21:32:52.986010Z",
            "type": "Property",
            "value": 60.0
        }
    ],
    "id": "urn:ngsi-ld:BeeHive:01",
    "temperature": [
        {
            "instanceId": "urn:ngsi-ld:Instance:33642ff7-9b66-42c4-bcdf-9ca640cba782",
            "observedAt": "2019-10-26T22:35:52.986010Z",
            "type": "Property",
            "value": 42.0
        },
        {
            "instanceId": "urn:ngsi-ld:Instance:73226f04-1c47-49a6-ab88-976cb7493bea",
            "observedAt": "2019-10-26T21:32:52.986010Z",
            "type": "Property",
            "value": 22.2
        }
    ],
    "type": "BeeHive"
}
```

* Let's update again the temperature property of the beehive 

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:$CONTEXT_LINK < samples/beehive_secondTemperatureUpdate.json
```

* We can get the simplified temporal representation of the temperature property of the beehive

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01?options=temporalValues \
    attrs==temperature \ 
    timerel==between \
    time==2019-10-25T12:00:00Z \
    endTime==2020-10-27T12:00:00Z \
    Link:$CONTEXT_LINK
```

The sample payload returned showing the simplified temporal evolution of temperature and humidity properties:

```json
{
    "@context": [
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld"
    ],
    "temperature": {
        "createdAt": "2020-06-15T11:37:25.803985Z",
        "observedBy": {
            "createdAt": "2020-06-15T11:37:25.830823Z",
            "object": "urn:ngsi-ld:Sensor:01",
            "type": "Relationship"
        },
        "type": "Property",
        "unitCode": "CEL",
        "values": [
            [
                42.0,
                "2019-10-26T22:35:52.986010Z"
            ],
            [
                22.2,
                "2019-10-26T21:32:52.986010Z"
            ],
            [
                100.0,
                "2020-05-10T10:20:30.98601Z"
            ]
        ]
    },
    "type": "BeeHive"
}
```
