# Quickstart

This quickstart guide shows a real use case scenario of interaction with the API in an Apiculture context.

## Prepare your environment

The provided examples make use of the HTTPie command line tool (installation instructions: [https://httpie.org/docs#installation](https://httpie.org/docs#installation))

All requests are grouped in a Postman collection that can be found [here](https://www.postman.com/stellio-doc/workspace/stellio/collection/34896864-4c88c4ee-aeb9-4851-b745-59f676e358d5?action=share&source=copy-link&creator=34896864).
For more details about how to import a Postman collection see [https://learning.postman.com/docs/getting-started/importing-and-exporting-data](https://learning.postman.com/docs/getting-started/importing-and-exporting-data).

Export the link to the JSON-LD context used in this use case in an environment variable for easier referencing in
the requests:

````shell
export CONTEXT_LINK="<https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""
````

## Starting the Stellio Context Broker
The Stellio Context Broker source code can be found here : [https://github.com/stellio-hub/stellio-context-broker](https://github.com/stellio-hub/stellio-context-broker).

To Start a Stellio instance. You can clone the Stellio repository and use the provided Docker compose configuration to run containers:

```shell
git clone https://github.com/stellio-hub/stellio-context-broker

cd stellio-context-broker

docker compose -f docker-compose.yml up -d && docker compose -f docker-compose.yml logs -f
```

## Case study

This case study is written for anyone who wants to get familiar with the API, we use a real example to make it more concrete.

We will use the following simple example of an apiary :
 - managed by a beekeeper
 - with a temperature observed by a sensor.

## Queries

### Create the entities

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

* Create the Sensor entity:
```shell
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json <<< '
{
   "id": "urn:ngsi-ld:Sensor:01",
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

* Create the Beehive entity:
```shell
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/ld+json <<< '
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
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
   }
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```

### Consume Entities

#### Retrieve Entity
The created beehive can be retrieved by id:

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01 Link:$CONTEXT_LINK
```
* The consumption endpoints support a lot of parameters
-  `format=keyValues` will return a reduced version of the entity providing only top level attribute and their value or object.
-  `join=inline` will join the relationships to the result.
-  `pick=id,temperature` will only return the selected attributes
-  `omit=temperature` will not returned the selected attributes

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01  options==keyValues Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

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
</details>

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01  join==inline Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

todo
</details>


```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01  pick==id,temperature Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

todo
</details>

#### Query Entity

You can also retrieve multiple entities via a query :

The query endpoint support different filter strategy
-  `type=Beehive|Sensor` only returned entities of type Beehive or Sensor (see [the specification #4.17](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.17))
-  `q=temperature>=22` only returned entities which temperature are superior to 22.2 (see [the specification #4.19](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.19))
-  `georel=near;maxDistance==1&geometry=Point&coordinates=[24.30623,60.07966]` let you find entities near a point (see [specification #4.10](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.10))

```shell
http http://localhost:8080/ngsi-ld/v1/entities type==BeeHive Link:$CONTEXT_LINK
http http://localhost:8080/ngsi-ld/v1/entities q==temperature>=22 Link:$CONTEXT_LINK
http http://localhost:8080/ngsi-ld/v1/entities 'georel==near;maxDistance==0' geometry==Point coordinates==[24.30623,60.07966] Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

todo
</details>

### Modify Entity 

#### Update Entity


#### Merge Entity


#### Delete Entity

If needed we can delete the created entities :

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Beekeeper:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01
```


### Batch Operations
#### Batch Create Entities

We can recreate multiple entities in one batch request :

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
        "id": "urn:ngsi-ld:Sensor:01",
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
        "@context": [
            "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
        ]
    }
]
'
```

### Discovery Endpoint
#### Discover types

All available types of entities can be retrieved:
```shell
http http://localhost:8080/ngsi-ld/v1/types Link:$CONTEXT_LINK
```

<details>
<Summary>Show response</Summary>

```json
{
    "id": "urn:ngsi-ld:EntityTypeList:e2d9b009-d2d3-45af-8adc-048a433a80a2",
    "type": "EntityTypeList",
    "typeList": [
        "Beekeeper",
        "BeeHive",
        "Apiary",
        "Sensor"
    ]
}
```
</details>

To get more details about a type of an entity, a Beekeeper for example:

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

#### Discover Attributes

-- todo link to the spec.


### Attribute endpoints
todo complete

#### Append Attributes on Entities
Let's add a name to the created beehive:

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
#### Delete attribute
The recently added name property can be deleted:

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/name Link:$CONTEXT_LINK
```

#### Update Attributes
We can also update the entire temperature attribute with:

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

#### Partial Attribute Update

We can update only part of the temperature attribute with:

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/temperature \
    Link:$CONTEXT_LINK <<<'
{
     "value": 42,
     "observedAt": "2019-10-26T22:35:52.98601Z"
}
'
```



### Consume Temporal Data

#### Retrieve Temporal Evolution of an entity

Since we updated the temperature, we can get the temporal evolution with:

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

We can get the simplified temporal representation of the temperature property of the beehive by adding the request parameter `format` with the value `temporalValues` in the query:

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01?options=temporalValues \
    option==temporalValues \
    attrs==temperature \ 
    timerel==between \
    timeAt==2019-10-25T12:00:00Z \
    endTimeAt==2020-10-27T12:00:00Z \
    Link:$CONTEXT_LINK
```

<details>
<Summary>Show response</Summary>

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
</details>

#### Query

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
