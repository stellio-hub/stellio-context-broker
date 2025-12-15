# Quickstart

This quickstart walks you through a realistic example of interacting with the API in an apiculture context.

## Prepare your environment

### Starting the Stellio Context Broker
The code of stellio can be found here: [https://github.com/stellio-hub/stellio-context-broker](https://github.com/stellio-hub/stellio-context-broker).

To start a local Stellio instance, clone the repository and use the provided Docker Compose configuration:

```shell
git clone https://github.com/stellio-hub/stellio-context-broker

cd stellio-context-broker

docker compose -f docker-compose.yml up -d && docker compose -f docker-compose.yml logs -f
```

### Sending requests
You can use one of the following tools to send requests:

#### HTTPie (command-line)
HTTPie works on Linux, macOS, and Windows (including WSL). Installation instructions: [https://httpie.org/docs#installation](https://httpie.org/docs#installation).

Export the link to the JSON-LD context used in this walkthrough to an environment variable for easier reuse in requests:

```shell
export CONTEXT_LINK="<https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""
```

#### Postman collection

You can also use our Postman collection to send the requests: <https://www.postman.com/stellio-doc/workspace/stellio/api/52d19e25-79fe-41a0-9646-4e30cc8ab2ab?action=share&creator=34896864>.

Note: The Postman web app cannot access `localhost`. To test against a local broker, install the Postman desktop app or use a broker with a public URL.

If you prefer importing the collection directly, use: <https://raw.githubusercontent.com/stellio-hub/stellio-docs/master/collection/API_walkthrough.json> (see Postman’s guide: <https://learning.postman.com/docs/getting-started/importing-and-exporting-data>).

## Case study

This case study is for anyone who wants to get familiar with the API. We use a simple, concrete example.

We will model the following scenario:

 - an apiary
   - managed by a beekeeper
   - with temperature observed by a sensor

## Create the entities

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
         "coordinates": [24.30623, 60.07966]
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

```

* Create the Beekeeper entity:
<details>
<Summary>Show request</Summary>

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
</details>
<br>

* Create the Sensor entity:

<details>
<Summary>Show request</Summary>

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
</details>

## Consume entities

### Retrieve an entity
You can retrieve the created beehive by ID:

```shell
http http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01 Link:$CONTEXT_LINK
```

<details>
<Summary>Show response</Summary>

````json
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
         "coordinates": [24.30623, 60.07966]
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
````
</details>
<br>

The consumption endpoints support several parameters:

- `format=keyValues` returns a reduced representation of the entity, keeping only top‑level attributes and their values or objects.
- `join=inline` includes related entities by inlining relationships.
- `pick=id,temperature` returns only the selected attributes.
- `omit=location,temperature` excludes the selected attributes from the response.

Note: These parameters also work for Query Entities and temporal retrieval operations.

<br>
Example using `keyValues`:

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
<br>

Example using `join`:
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
<br>

Example using `pick`:

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
<br>
Example using `omit`:

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
<br>

### Query entities

You can retrieve multiple entities with a query.

The query endpoint supports several filtering strategies:

- `type=Beehive` returns only entities of type `BeeHive` (see [specification §4.17](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.17)).
- `q=temperature>=22` returns only entities with a temperature greater than or equal to 22 (see [specification §4.19](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.19)).
- `georel=near;maxDistance==1&geometry=Point&coordinates=[24.30623,60.07966]` finds entities near a point (see [specification §4.10](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.10)).

```shell
http http://localhost:8080/ngsi-ld/v1/entities type==BeeHive,Sensor Link:$CONTEXT_LINK
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
<br>

### Query by POST
If the request parameters are too long, use `entityOperations/query` for a [query by POST (§6.23)](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.23).
Pass the parameters in the request body as a [Query object (§5.2.23)](https://cim.etsi.org/NGSI-LD/official/clause-5.html#5.2.23).

## Modify an entity
### Replace an entity 
This endpoint replaces the entire entity with the new payload.

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
Fetch the entity to see the result: [Consume entities](#consume-entities).
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
<br>


### Merge an entity
This endpoint updates only the attributes present in the payload.

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
Fetch the entity to see the result: [Consume entities](#consume-entities).

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
<br>
### Delete an entity

Delete the created entities if needed:

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:01
```
Note: This endpoint keeps the historical representation of the entity and marks it as deleted.
To permanently delete an entity, use [Temporal delete](#permanently-delete-entity).

## Batch operations
### Batch create entities

Create multiple entities in a single batch request:

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
### Batch entity creation or update (upsert)
This endpoint creates each entity in the payload, or updates it if it already exists.

`POST entityOperations/upsert`
You can also specify `options=replace` or `options=update` (see [§6.15](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.15)).

### Batch entity update
This endpoint updates each entity in the payload.

`POST entityOperations/update`
You can also specify the `noOverwrite` parameter (see [§6.16](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.16)).

### Batch entity merge
This endpoint merges each entity in the payload with the existing one (see [§6.31](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.31)).

`POST entityOperations/merge`

### Batch entity delete
This endpoint receives a list of IDs and deletes the corresponding entities (see [§6.17](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.17)).

`POST entityOperations/delete`

## Attribute endpoints

### Append attributes to an entity
Add a name to the created beehive:

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

### Update attributes
Update the entire `temperature` attribute:

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

### Partially update an attribute

Update only part of the `temperature` attribute:

```shell
http PATCH http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/temperature \
    Link:$CONTEXT_LINK <<<'
{
     "value": 42,
     "observedAt": "2025-10-12T10:20:30.98601Z"
}
'
```

### Replace an attribute
You can completely replace an attribute with `PUT /entities/urn:ngsi-ld:BeeHive:01/attrs/temperature` (see [§6.7.3.3](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.7.3.3)).

### Delete an attribute
Delete the `name` property added earlier:

```shell
http DELETE http://localhost:8080/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/name Link:$CONTEXT_LINK
```



## Consume temporal data
### Retrieve an entity’s temporal evolution

Since we updated the temperature, retrieve the temporal evolution with:

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
<br>
The temporal endpoints support several parameters:

- `format=temporalValues` reduces the payload size by returning the temporal representation as a list of pairs [value, timestamp].
- `timerel==after&timeAt==2025-26-10T12:00:00Z` returns temporal values from the specified time onwards (see [§4.11](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.11)).
- `lastN=2` returns only the last two values.
- `format=aggregatedValues&aggrMethods=max,avg&aggrPeriodDuration=P1D` calculates the maximum, average, and counts values per day (see [§4.5.19](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.5.19.1)).

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
<br>

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
<br>


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
<br>

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
<br>

### Query temporal evolution of entities
You can also query multiple temporal entities:

```shell
http http://localhost:8080/ngsi-ld/v1/temporal/entities \
  type=BeeHive \
  timerel=after \
  timeAt=2025-10-26T12:00:00Z \
  Link:$CONTEXT_LINK
```
Note 1: This endpoint supports the parameters from Query Entity and Retrieve Temporal Entity.
Note 2: You must provide a time filter (`timerel`, `timeAt`) and an entity filter (`q`, `type`, `geoQ`, `attrs`, or `local=true`).

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
The list contains multiple values if multiple entities match the request.
</details>

## Modify temporal data
### Permanently delete an entity
Delete an entity and all its history with:
```shell
http DELETE http://localhost:8080/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01
```

### Other endpoints
Other temporal provisioning endpoints are described in [§§6.20–6.22 of the specification](https://cim.etsi.org/NGSI-LD/official/clause-6.html#6.20).


## Discovery endpoint
### Discover types

Retrieve all available entity types with:
```shell
http http://localhost:8080/ngsi-ld/v1/types Link:$CONTEXT_LINK
```
Note: supports `details=true` for additional information.

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
<br>

Retrieve additional details about a specific entity type with:

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
<br>

### Discover attributes
Retrieve all available attributes across entities with:
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
<br>

Retrieve more details about a specific attribute with:
```shell
http http://localhost:8080/ngsi-ld/v1/attributes/name Link:$CONTEXT_LINK
```
Note: supports `details=true` for additional information.

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
<br>

## Subscription

### Create a subscription 
Create a subscription to be notified when the temperature of the `BeeHive` exceeds 40. You need a working endpoint to receive the notification.
For this example, use Post Server V2 (<http://ptsv2.com/>). Configure an appropriate endpoint for your own data.
The endpoint supports HTTP or MQTT.

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
### Retrieve a subscription
Retrieve a subscription by ID:

```shell
http http://localhost:8080/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Link:$CONTEXT_LINK
```

### Trigger a notification
Running the previous partial update [query](#partially-update-an-attribute) after creating the subscription sends a notification to the configured endpoint.
The body of the notification sent to the endpoint URI is:

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
For more information, see the [authentication and authorisation guide](authentication_and_authorization.md).
