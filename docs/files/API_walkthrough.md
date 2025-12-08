# Quickstart

This quickstart guide shows a real use case scenario of interaction with the API in an Apiculture context.

## Prepare your environment

The provided examples make use of the HTTPie command line tool (installation instructions: [https://httpie.org/docs#installation](https://httpie.org/docs#installation))

-- todo
All requests are grouped in a Postman collection that can be found [here](https://raw.githubusercontent.com/stellio-hub/stellio-docs/master/collection/API_Quick_Start.postman_collection.json).
For more details about how to import a Postman collection see [https://learning.postman.com/docs/getting-started/importing-and-exporting-data](https://learning.postman.com/docs/getting-started/importing-and-exporting-data).

Export the link to the JSON-LD context used in this use case in an environment variable for easier referencing in
the requests:

````shell
export CONTEXT_LINK="<https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""
````

## Starting the Stellio Context Broker
The Stellio Context Broker source code can be found here : https://github.com/stellio-hub/stellio-context-broker

To Start a Stellio instance. You can clone the Stellio repository and use the provided Docker compose configuration to run containers:

```shell
git clone https://github.com/stellio-hub/stellio-context-broker

cd stellio-context-broker

docker compose -f docker-compose.yml up -d && docker compose -f docker-compose.yml logs -f
```

## Case study

This case study is written for anyone who wants to get familiar with the API, we use a real example to make it more concrete.

We will create the following entities:

- Beekeeper

- Apiary

- Sensor temperature

- Sensor humidity

- Beehive


## Queries

### Create Entities

* Create the beekeeper entity:

```shell
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json <<< '
{
   "id":"urn:ngsi-ld:Beekeeper:01",
   "type":"Beekeeper",
   "name":{
       "type":"Property",
       "value":"Scalpa"
   },
   "@context":[
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```

* Create the Apiary entity:
```shell
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json <<< '
{
   "id":"urn:ngsi-ld:Apiary:01",
   "type":"Apiary",
   "name":{
      "type":"Property",
      "value":"ApiarySophia"
   },
   "location": {
      "type": "GeoProperty",
      "value": {
         "type": "Point",
         "coordinates": [
            24.30623,
            60.07966
         ]
      }
   },
   "@context":[
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```

* Create the Sensor_temperature entity:
```shell
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json <<< '
{
   "id": "urn:ngsi-ld:Sensor:02",
   "type": "Sensor",
   "deviceParameter":{
         "type":"Property",
         "value":"temperature"
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```

* Create the Sensor_humidity entity:
```shell
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json <<< '
{
   "id": "urn:ngsi-ld:Sensor:01",
   "type": "Sensor",
   "deviceParameter":{
         "type":"Property",
         "value":"humidity"
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```

* Create the Beehive entity:
```shell
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json <<< '
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "belongs": {
      "type": "Relationship",
      "object": "urn:ngsi-ld:Apiary:01"
   },
   "managedBy": {
     "type": "Relationship",
     "object": "urn:ngsi-ld:Beekeeper:01"
   },
   "location": {
      "type": "GeoProperty",
      "value": {
         "type": "Point",
         "coordinates": [
            24.30623,
            60.07966
         ]
      }
   },
   "temperature": {
      "type": "Property",
      "value": 22.2,
      "unitCode": "CEL",
      "observedAt": "2019-10-26T21:32:52.98601Z",
      "observedBy": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:Sensor:01"
      }
   },
   "humidity": {
      "type": "Property",
      "value": 60,
      "unitCode": "P1",
      "observedAt": "2019-10-26T21:32:52.98601Z",
      "observedBy": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:Sensor:02"
      }
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```

### Delete Entities

* We can delete the created entities (optional):

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Beekeeper:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Apiary:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:02
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01
```

### Batch Create Entities

* We can recreate them in batch (optional):

```shell
http POST http://localhost:8080/ngsi-ld/v1/entityOperations/create Content-Type:application/ld+json <<<'
[
    {
        "id":"urn:ngsi-ld:Beekeeper:01",
        "type":"Beekeeper",
        "name":{
            "type":"Property",
            "value":"Scalpa"
        },
        "@context":[
            "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
        ]
    },
    {
        "id":"urn:ngsi-ld:Apiary:01",
        "type":"Apiary",
        "name":{
            "type":"Property",
            "value":"ApiarySophia"
        },
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [
                    24.30623,
                    60.07966
                ]
            }
        },
        "@context":[
            "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
        ]
    },
    {
        "id": "urn:ngsi-ld:Sensor:01",
        "type": "Sensor",
        "deviceParameter":{
                "type":"Property",
                "value":"humidity"
        },
        "@context": [
            "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
        ]
    },
    {
        "id": "urn:ngsi-ld:Sensor:02",
        "type": "Sensor",
        "deviceParameter":{
                "type":"Property",
                "value":"temperature"
        },
        "@context": [
            "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
        ]
    },
    {
        "id": "urn:ngsi-ld:BeeHive:01",
        "type": "BeeHive",
        "belongs": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Apiary:01"
        },
        "managedBy": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Beekeeper:01"
        },
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [
                    24.30623,
                    60.07966
                ]
            }
        },
        "temperature": {
            "type": "Property",
            "value": 22.2,
            "unitCode": "CEL",
            "observedAt": "2019-10-26T21:32:52.98601Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        },
        "humidity": {
            "type": "Property",
            "value": 60,
            "unitCode": "P1",
            "observedAt": "2019-10-26T21:32:52.98601Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:02"
            }
        },
        "@context": [
            "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
        ]
    }
]
'
```

### Discover Entities

* All available types of entities can be retrieved:
```shell
http http://localhost:8080/ngsi-ld/v1/types
```

Sample payload returned showing the current available entity types:

```json
{
    "id": "urn:ngsi-ld:EntityTypeList:e2d9b009-d2d3-45af-8adc-048a433a80a2",
    "type": "EntityTypeList",
    "typeList": [
        "https://ontology.eglobalmark.com/apic#Beekeeper",
        "https://ontology.eglobalmark.com/apic#BeeHive",
        "https://ontology.eglobalmark.com/apic#Apiary",
        "https://ontology.eglobalmark.com/egm#Sensor"
    ]
}
```

* To get more details about a type of an entity, a Beekeeper for example:

```shell
http http://localhost:8080/ngsi-ld/v1/types/Beekeeper Link:$CONTEXT_LINK
```

Sample payload returned showing more details about the entity Beekeeper:

```json
{
    "id": "https://ontology.eglobalmark.com/apic#Beekeeper",
    "type": "EntityTypeInfo",
    "typeName": "Beekeeper",
    "entityCount": 1,
    "attributeDetails": [
        {
            "id": "https://uri.etsi.org/ngsi-ld/name",
            "type": "Attribute",
            "attributeName": "name",
            "attributeTypes": [
                "Property"
            ]
        }
    ]
}
```

### Query Entities

* The created beehive can be retrieved by id:

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01 Link:$CONTEXT_LINK
```

* Or by querying all entities of type BeeHive:

```shell
http http://localhost:8080/ngsi-ld/v1/entities type==BeeHive Link:$CONTEXT_LINK
```

* Adding the options `keyValues` to the request parameters will return a reduced version of the entity providing only top level attribute and their value or object.

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01  options==keyValues Link:$CONTEXT_LINK
```
Sample payload returned showing a reduced version of the entity BeeHive:

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
    "humidity": 60,
    "temperature": 22.2,
    "belongs": "urn:ngsi-ld:Apiary:01",
    "managedBy": "urn:ngsi-ld:Beekeeper:01",
    "location": {
        "type": "Point",
        "coordinates": [
            24.30623,
            60.07966
        ]
    },
    "@context": [
        "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
    ]
}
```

### Append Attributes on Entities

* Let's add a name to the created beehive:

```shell
http POST http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:$CONTEXT_LINK <<<'
{
   "name":{
      "type":"Property",
      "value":"BeeHiveSophia"
   }
}
'
```

* The recently added name property can be deleted:

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/name Link:$CONTEXT_LINK
```

### Partial Attributes Updates

* We can update the temperature with a value greater than 40, using the following query:

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/temperature \
    Link:$CONTEXT_LINK <<<'
{
     "value": 42,
     "observedAt": "2019-10-26T22:35:52.98601Z"
}
'
```

* We can also update the humidity of the beehive:

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:$CONTEXT_LINK <<<'
{
   "humidity": {
     "type": "Property",
     "value": 58,
     "unitCode": "P1",
     "observedAt": "2019-10-26T21:35:52.98601Z",
     "observedBy": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Sensor:02"
     }
  }
}
'
```

### Get Temporal Evolution of Attributes

* Since we updated both temperature and humidity, we can get the temporal evolution of those properties:

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 \
    timerel==between \
    timeAt==2019-10-25T12:00:00Z \
    endTimeAt==2021-10-27T12:00:00Z \
    Link:$CONTEXT_LINK
```

Sample payload returned showing the temporal evolution of temperature and humidity properties:

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
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
    "@context": [
        "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
    ]
}
```

* Let's update again the temperature property of the beehive:

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:$CONTEXT_LINK <<< '
{
   "temperature": {
     "type": "Property",
     "value": 100,
     "unitCode": "CEL",
     "observedAt": "2020-05-10T10:20:30.98601Z",
     "observedBy": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Sensor:01"
     }
   }
}
'
```

* We can get the simplified temporal representation of the temperature property of the beehive by adding the request parameter `option` with the value `temporalValues` in the query:

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01?options=temporalValues \
    option==temporalValues \
    attrs==temperature \ 
    timerel==between \
    timeAt==2019-10-25T12:00:00Z \
    endTimeAt==2020-10-27T12:00:00Z \
    Link:$CONTEXT_LINK
```

The sample payload returned showing the simplified temporal evolution of temperature and humidity properties:

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
    "temperature": {
        "type": "Property",
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
    "@context": [
        "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
    ]
}
```

### Subscription

* Let's create a subscription on the Beehive entity. We would like to be notified when the temperature of the BeeHive exceeds 40. To do so, we need a working `endpoint` in order to receive the notification related to the subscription. For this example, we are using Post Server V2 [http://ptsv2.com/](http://ptsv2.com/). This is a free public service. It is used only for tests and debugs. You need to configure your appropriate working `endpoint` for your private data.

```shell
http POST http://localhost:8080/ngsi-ld/v1/subscriptions Content-Type:application/ld+json <<<'
{
  "id":"urn:ngsi-ld:Subscription:01",
  "type":"Subscription",
  "entities": [
    {
      "type": "BeeHive"
    }
  ],
  "q": "temperature>40",
  "notification": {
    "attributes": ["temperature"],
    "format": "normalized",
    "endpoint": {
      "uri": "http://ptsv2.com/t/ir6uu-1626449561/post",
      "accept": "application/json"
    }
  },
  "@context": [
     "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
  ]
}
'
```

* The created subscription can be retrieved by id:

```shell
http http://localhost:8080/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Link:$CONTEXT_LINK
```

Running the previous partial update [query](#partial-attributes-updates) (after the creation of the subscription), will trigger sending notification to the configured endpoint. The body of the notification query sent to the endpoint URI is:

```json
{
    "id": "urn:ngsi-ld:Notification:65925510-b64c-4bfa-942d-395803a9a784",
    "type": "Notification",
    "subscriptionId": "urn:ngsi-ld:Subscription:01",
    "notifiedAt": "2021-07-16T15:51:54.654005Z",
    "data": [
        {
            "id": "urn:ngsi-ld:BeeHive:01",
            "type": "BeeHive",
            "temperature": {
                "type": "Property",
                "observedBy": {
                    "type": "Relationship",
                    "createdAt": "2021-07-16T15:51:54.215352Z",
                    "object": "urn:ngsi-ld:Sensor:01"
                },
                "createdAt": "2021-07-16T15:51:54.215300Z",
                "value": 42,
                "observedAt": "2019-10-26T22:35:52.986010Z",
                "unitCode": "CEL"
            },
            "@context": [
                "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
            ]
        }
    ]
}
```
