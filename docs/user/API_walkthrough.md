# Quickstart

This quickstart guide shows a real use case scenario of interaction with the API in an Apiculture context.

## Prepare your environment

### Starting the Stellio Context Broker
The source code can be found here : [https://github.com/stellio-hub/stellio-context-broker](https://github.com/stellio-hub/stellio-context-broker).

To Start a Stellio instance. You can clone the repository and use the provided Docker compose configuration to run containers:

```shell
git clone https://github.com/stellio-hub/stellio-context-broker

cd stellio-context-broker

docker compose -f docker-compose.yml up -d && docker compose -f docker-compose.yml logs -f
```

### Launching requests
You have multiple solutions to launch the requests :

#### HTTPie command line tool
The HTTPie command line tool, works with a linux terminal including mac-os and WSL (installation instructions: [https://httpie.org/docs#installation](https://httpie.org/docs#installation)). 

Export the link to the JSON-LD context used in this use case in an environment variable for easier referencing in the requests:

```shell
export CONTEXT_LINK="<https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""
```

#### Postman collection

The [postman collection](https://www.postman.com/stellio-doc/workspace/stellio/api/52d19e25-79fe-41a0-9646-4e30cc8ab2ab?action=share&creator=34896864).
can be used to launch the request. 
The postman website is unable to reach your localhost, you will need to install postman or use a broker with a public url. 

If you prefer [importing the collection](https://learning.postman.com/docs/getting-started/importing-and-exporting-data)
directly, the [exported collection](https://raw.githubusercontent.com/stellio-hub/stellio-docs/master/collection/API_walkthrough.json)
is available.

## Case study

This case study is written for anyone who wants to get familiar with the API, we use a real example to make it more concrete.

We will use the following simple example:

 - an apiary
   - managed by a beekeeper
   - with a temperature observed by a sensor.

## Create the entities

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
      "observedAt": "2025-10-26T21:32:52.98601Z",
      "observedBy": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:Sensor:01"
      }
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```

## Consume Entities

### Retrieve Entity
The created beehive can be retrieved by id:

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01 Link:$CONTEXT_LINK
```

<details>
<Summary>Show response</Summary>

````json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "ngsi-ld:default-context/BeeHive",
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
    "ngsi-ld:default-context/humidity": {
        "type": "Property",
        "value": 60,
        "unitCode": "P1",
        "observedAt": "2025-10-26T21:32:52.98601Z",
        "ngsi-ld:default-context/observedBy": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Sensor:01"
        }
    },
    "ngsi-ld:default-context/managedBy": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Beekeeper:01"
    }
}
````
</details>

The consumption endpoints support a lot of parameters:

-  `format=keyValues` will return a reduced version of the entity providing only top level attribute and their value or object.
-  `join=inline` will join the relationships to the result.
-  `pick=id,temperature` will only return the selected attributes
-  `omit=location,temperature` will not returned the selected attributes

Note: These parameters also work for Query Entities and temporal retrieve operations 

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01  options==keyValues Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
    "temperature": 22.2,
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

```json 
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
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
    "managedBy": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Beekeeper:01",
        "entity": {
            "id": "urn:ngsi-ld:Beekeeper:01",
            "type": "Beekeeper",
            "name": {
                "type": "Property",
                "value": "Scalpa"
            }
        }
    },
    "temperature": {
        "type": "Property",
        "value": 22.2,
        "unitCode": "CEL",
        "observedAt": "2025-10-26T21:32:52.98601Z",
        "observedBy": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Sensor:01"
        }
    }
}
```
</details>


```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01  pick==id,temperature Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "temperature": {
        "type": "Property",
        "value": 22.2,
        "unitCode": "CEL",
        "observedAt": "2025-10-26T21:32:52.98601Z",
        "observedBy": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Sensor:01"
        }
    }
}
```
</details>

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01  omit==temperature,location Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
    "managedBy": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Beekeeper:01"
    }
}
```
</details>

### Query Entities

You can also retrieve multiple entities via a query :

The query endpoint support different filter strategy

-  `type=Beehive` only returned entities of type Beehive or Sensor (see [the specification #4.17](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.17))
-  `q=temperature>=22` only returned entities which temperature are superior to 22 (see [the specification #4.19](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.19))
-  `georel=near;maxDistance==1&geometry=Point&coordinates=[24.30623,60.07966]` let you find entities near a point (see [specification #4.10](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.10))

```shell
http http://localhost:8080/ngsi-ld/v1/entities type==BeeHive Link:$CONTEXT_LINK
http http://localhost:8080/ngsi-ld/v1/entities q==temperature>=22 Link:$CONTEXT_LINK
http http://localhost:8080/ngsi-ld/v1/entities 'georel==near;maxDistance==1' geometry==Point coordinates==[24.30623,60.07966] Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

```json 
[
    {
        "id": "urn:ngsi-ld:Sensor:01",
        "type": "Sensor",
        "deviceParameter": {
            "type": "Property",
            "value": "temperature"
        }
    },
    {
        "id": "urn:ngsi-ld:BeeHive:01",
        "type": "BeeHive",
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
        "managedBy": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Beekeeper:01"
        },
        "temperature": {
            "type": "Property",
            "value": 22.2,
            "unitCode": "CEL",
            "observedAt": "2025-10-26T21:32:52.98601Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        }
    }
]
```
</details>

### Query By post
If the request parameter is too long you can use the url `entityOperation/query` to do a [query by post #6.23](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.23).
The request param will be defined in the body of type [query object #5.2.23](https://cim.etsi.org/NGSI-LD/official/clause-5.html#5.2.23).

## Modify Entity
### Replace Entity 
The replace entity endpoint will override the whole entity with the new payload.

```shell
http PUT http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01 Content-Type:application/ld+json <<< '
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
   "temperature": {
      "type": "Property",
      "value": 43.2,
      "unitCode": "CEL",
      "observedAt": "2025-10-26T21:32:52.98601Z",
      "observedBy": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:Sensor:01"
      }
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```
The result is visible when we [fetch the entity](#consume-entities).
<details>
<Summary>Show response</Summary>

```json 
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "temperature": {
      "type": "Property",
      "value": 43.2,
      "unitCode": "CEL",
      "observedAt": "2025-10-26T21:32:52.98601Z",
      "observedBy": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:Sensor:01"
      }
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```
</details>


### Merge Entity
The Merge entity will only update attributes who are present in the payload.

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01 Content-Type:application/ld+json <<< '
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "managedBy": {
       "type": "Relationship",
       "object": "urn:ngsi-ld:Beekeeper:01",
        "subProperty": {
            "type": "Property",
            "value": "addSubProperty"
        }
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
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
'
```
The result is visible when we [fetch the entity](#consume-entities).

<details>
<Summary>Show response</Summary>

```json 
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
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
    "managedBy": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Beekeeper:01",
        "subProperty": {
            "type": "Property",
            "value": "addSubProperty"
        }
    },
    "temperature": {
        "type": "Property",
        "value": 43.2,
        "unitCode": "CEL",
        "observedAt": "2025-10-26T21:32:52.98601Z",
        "observedBy": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:Sensor:01"
        }
    },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```
</details>
### Delete Entity

If needed we can delete the created entities :

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:01
```
Note : This endpoint keep the historical representation of the entity and mark is as deleted.
 If you want to permanently delete an entity you should use the [Temporal delete](#permanently-delete-entity).

## Batch Operations
### Batch Create Entities

We can recreate multiple entities in one batch request :

```shell
http POST http://localhost:8080/ngsi-ld/v1/entityOperations/create Content-Type:application/ld+json <<<'
[
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
            "observedAt": "2025-10-26T21:32:52.98601Z",
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
### Batch Entity creation or update (upsert)
The endpoint create every entity in the payload or update them if they already existed. 

`POST entityOperations/upsert`
Additionally we can specify `options=replace` or `option=update` see [#6.15](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.15)

### Batch Entity update
The endpoint update every entity in the payload. 

`POST entityOperations/update`
Additionally we can specify the `noOverwrite` parameter see [#6.16](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.16)

### Batch Entity merge
The endpoint merge every entity in the payload with their existing entity see [#6.31](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.31).

`POST entityOperations/merge`

### Batch Entity delete
The endpoint receive a list of id and delete the corresponding entities see [#6.17](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.17).

`POST entityOperations/delete`

## Attribute endpoints

### Append Attributes on Entities
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

### Update Attributes
We can update the entire temperature attribute with:

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:$CONTEXT_LINK <<<'
{
   "temperature": {
     "type": "Property",
     "value": 30.7,
     "unitCode": "P1",
     "observedAt": "2025-10-26T21:35:52.98601Z",
     "observedBy": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Sensor:01"
     }
  }
}
'
```

### Partial Attribute Update

We can update only part of the temperature attribute with:

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/temperature \
    Link:$CONTEXT_LINK <<<'
{
     "value": 42,
     "observedAt": "2025-10-12T10:20:30.98601Z"
}
'
```

### Replace attribute
We can also completely replace an attribute with `PUT /entities/urn:ngsi-ld:BeeHive:01/attrs/temperature` see [#6.7.3.3](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.7.3.3)

### Delete attribute
The recently added name property can be deleted:

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/name Link:$CONTEXT_LINK
```



## Consume Temporal Data
### Retrieve Temporal Evolution of an entity

Since we updated the temperature, we can get the temporal evolution with:

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01  Link:$CONTEXT_LINK
```
<details>
<Summary>Show response</Summary>

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
    "temperature": [
        {
            "type": "Property",
            "value": 2,
            "unitCode": "CEL",
            "instanceId": "urn:ngsi-ld:Instance:29b397cd-8c95-42c5-94a4-6d8928bfda4a",
            "observedAt": "2025-10-12T10:20:30.986010Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        },
        {
            "type": "Property",
            "value": 22.1,
            "unitCode": "CEL",
            "instanceId": "urn:ngsi-ld:Instance:03b60d8c-1fde-4acb-853c-6ebf2697675c",
            "observedAt": "2025-10-26T21:32:52.986010Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        },
        {
            "type": "Property",
            "value": 43,
            "unitCode": "CEL",
            "instanceId": "urn:ngsi-ld:Instance:7844b05f-8b71-46e8-9cbb-ec989448ec98",
            "observedAt": "2025-10-26T22:35:52.986010Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        }
    ],
    "@context": [
        "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
    ]
}
```
</details>
The temporal consumption endpoints support a lot of parameters:

-  `format=temporalValues` will reduce the payload size by returning the temporal representation as a list of Pair [value, timestamp].
-  `timerel==after&timeAt==2025-26-10T12:00:00Z` will only return temporal value from 2025 and forward, see [#4.11](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.11) for more details.
-  `lastN=2` will only return the last 2 values
-  `format=aggregatedValues&aggrMethods=max,avg&aggrPeriodDuration=P1D` will calculate the max, average and will count values for each day, see [#4.5.19](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.5.19.1) for more details.

Note: These parameters also work for Query Temporal Entities.
```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 format==temporalValues Link:$CONTEXT_LINK
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
                 2.0,
                 "2025-10-12T10:20:30.98601Z"
             ],
             [
                 22.1,
                 "2025-10-26T21:32:52.98601Z"
             ],
             [
                 43.0,
                 "2025-10-26T22:35:52.98601Z"
             ]
         ]
     },
    "@context": [
        "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
    ]
}
```
</details>

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 timerel==after  timeAt==2025-10-26T12:00:00Z Link:$CONTEXT_LINK
```

<details>
<Summary>Show response</Summary>

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
    "temperature": [
        {
            "type": "Property",
            "value": 22.1,
            "unitCode": "CEL",
            "instanceId": "urn:ngsi-ld:Instance:03b60d8c-1fde-4acb-853c-6ebf2697675c",
            "observedAt": "2025-10-26T21:32:52.986010Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        },
        {
            "type": "Property",
            "value": 43,
            "unitCode": "CEL",
            "instanceId": "urn:ngsi-ld:Instance:7844b05f-8b71-46e8-9cbb-ec989448ec98",
            "observedAt": "2025-10-26T22:35:52.986010Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        }
    ],
    "@context": [
        "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
    ]
}
```
</details>


```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 lastN==2 Link:$CONTEXT_LINK
```

<details>
<Summary>Show response</Summary>

```json
{
    "id": "urn:ngsi-ld:BeeHive:01",
    "type": "BeeHive",
    "temperature": [
        {
            "type": "Property",
            "value": 22.1,
            "unitCode": "CEL",
            "instanceId": "urn:ngsi-ld:Instance:03b60d8c-1fde-4acb-853c-6ebf2697675c",
            "observedAt": "2025-10-26T21:32:52.986010Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        },
        {
            "type": "Property",
            "value": 43,
            "unitCode": "CEL",
            "instanceId": "urn:ngsi-ld:Instance:7844b05f-8b71-46e8-9cbb-ec989448ec98",
            "observedAt": "2025-10-26T22:35:52.986010Z",
            "observedBy": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Sensor:01"
            }
        }
    ],
    "@context": [
        "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
    ]
}
```
</details>

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 \
format==aggregatedValues \
aggrMethods==max,avg,totalCount \
aggrPeriodDuration==P1D \
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
        "max": [
            [
                2.0,
                "2025-10-12T10:20:30.98601Z",
                "2025-10-13T10:20:30.98601Z"
            ],
            [
                43.0,
                "2025-10-26T10:20:30.98601Z",
                "2025-10-27T10:20:30.98601Z"
            ]
        ],
        "avg": [
            [
                2.0,
                "2025-10-12T10:20:30.98601Z",
                "2025-10-13T10:20:30.98601Z"
            ],
            [
                32.55,
                "2025-10-26T10:20:30.98601Z",
                "2025-10-27T10:20:30.98601Z"
            ]
        ],
        "totalCount": [
            [
                1,
                "2025-10-12T10:20:30.98601Z",
                "2025-10-13T10:20:30.98601Z"
            ],
            [
                2,
                "2025-10-26T10:20:30.98601Z",
                "2025-10-27T10:20:30.98601Z"
            ]
        ]
    },
    "@context": [
        "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
    ]
}
```
</details>

### Query Temporal evolution of entities
We can also Query multiple temporal entities :

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities \
  type=BeeHive \
  timerel=after \
  timeAt=2025-10-26T12:00:00Z \
  Link:$CONTEXT_LINK
```
Note 1: The endpoint support the parameter from Query Entity and Retrieve Temporal Entity
Note 2: It is mandatory to provide a time filter (timerel, timeAt) and an entity filter (q, type, geoQ, attrs or local=true)

<details>
<Summary>Show response</Summary>

```json
[
    {
        "id": "urn:ngsi-ld:BeeHive:01",
        "type": "BeeHive",
        "temperature": [
            {
                "type": "Property",
                "value": 22.1,
                "unitCode": "CEL",
                "instanceId": "urn:ngsi-ld:Instance:03b60d8c-1fde-4acb-853c-6ebf2697675c",
                "observedAt": "2025-10-26T21:32:52.986010Z",
                "observedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Sensor:01"
                }
            },
            {
                "type": "Property",
                "value": 43,
                "unitCode": "CEL",
                "instanceId": "urn:ngsi-ld:Instance:7844b05f-8b71-46e8-9cbb-ec989448ec98",
                "observedAt": "2025-10-26T22:35:52.986010Z",
                "observedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Sensor:01"
                }
            }
        ],
        "@context": [
            "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
        ]
    }
]
```
The list will contain multiple values if multiple entity match the request.
</details>

## Modify Temporal Data
### Permanently delete entity
You can delete an entity and all its history with :
```shell
http DELETE http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01
```

### Other Endpoints
Other temporal provision endpoint are described in [sections #6.20 to #6.22 of the specification](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.20)


## Discovery Endpoint
### Discover types

All available types of entities can be retrieved:
```shell
http http://localhost:8080/ngsi-ld/v1/types Link:$CONTEXT_LINK
```
Note: support `details=true` for additional information.

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

To get more details about a type of entity :

```shell
http http://localhost:8080/ngsi-ld/v1/types/Beekeeper Link:$CONTEXT_LINK
```

<details>
<Summary>Show response</Summary>

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
</details>

### Discover Attributes
All available attributes inside entities can be retrieved:
```shell
http http://localhost:8080/ngsi-ld/v1/attributes Link:$CONTEXT_LINK
```

<details>
<Summary>Show response</Summary>

```json
{
    "id": "urn:ngsi-ld:AttributeList:f0f85f20-5fa1-431b-83d9-9a61703d7a31",
    "type": "AttributeList",
    "attributeList": [
        "deviceParameter",
        "name"
    ]
}
```
</details>

More details about a specific attribute can be retrieved with ::
```shell
http http://localhost:8080/ngsi-ld/v1/attributes/name Link:$CONTEXT_LINK
```
Note: support `details=true` for additional information.

<details>
<Summary>Show response</Summary>

```json
{
    "id": "https://schema.org/name",
    "type": "Attribute",
    "attributeName": "name",
    "attributeTypes": [
        "Property"
    ],
    "typeNames": [
        "Beekeeper"
    ],
    "attributeCount": 1
}
```
</details>

## Subscription

### Create a subscripion 
We can ask to be notified when the temperature of the BeeHive exceeds 40. To do so, we need a working `endpoint` in order to receive the notification.
For this example, we are using Post Server V2 [http://ptsv2.com/](http://ptsv2.com/). You will need to configure an appropriate working `endpoint` for your private data.
The endpoint support http or mqtt address.

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
### Retrieve a Subscription
We can retrieve a subscription by id:

```shell
http http://localhost:8080/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Link:$CONTEXT_LINK
```

### Triggering a notification
Running the previous partial update [query](#partial-attribute-update) (after the creation of the subscription), will trigger sending notification to the configured endpoint. The body of the notification query sent to the endpoint URI is:

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
                "observedAt": "2025-10-26T22:35:52.986010Z",
                "unitCode": "CEL"
            },
            "@context": [
                "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
            ]
        }
    ]
}
```

## Going further
To go further you can follow the [authentication and authorization guide](authentication_and_authorization.md)