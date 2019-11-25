# Quick start

### Context Registry

This module is in charge of store, update and retrieve the NGSI-LD entities to/from a NEO4J database.  These are the API endpoint implemented:

| POST   | /v2/entities                                     |
| ------ | ------------------------------------------------ |
| POST   | /v2/entities                                     |
| PATCH  | /v2/entities/{entityId}/attrs/                   |
| POST   | /v2/entities/{entityId}/attrs/ (ONGOING)         |
| PATCH  | /v2/entities/{entityId}/attrs/{attrId}           |
| DELETE | /v2/entities/{entityId}/attrs/{attrId} (ONGOING) |
| DELETE | /v2/entities/{entityId} (ONGOING)                |
| GET    | /v2/entities                                     |
| GET    | /v2/entities/{entityId}                          |

  

#### NGSILD Entity structure

The NGSI-LD format is JSON with reserved keyword @context to add links to the ontologies of reference. The structure have to comply with certains requirements amongst them:

- an entity must have an id (represented by uri:namespace:EntityType:uuid) 
- an entity must have a type
- Relationship and Property are reserved types (they both ar Properties in the ngsild ontology)
- a Relationship must have an 'object' field that point to another Entity or Relationship
- 'Relationship of Relationship', 'Property of Property' , 'Property of Relationship' , 'Relationship of Property' are allowed

```
{
  "id": "urn:ngsi-ld:Vehicle:A1234",
  "type": "Vehicle",
  "brandName": {
    "type": "Property",
    "value": "Mercedes"
  },
  "name":  "name of vehicle 2",
  "isParked": {
    "type": "Relationship",
    "object": "urn:ngsi-ld:OffStreetParking:Downtown2",
    "observedAt": "2019-10-22T12:00:04Z",
    "providedBy": {
      "type": "Relationship",
      "object": "urn:ngsi-ld:Person:Bob"
    }
  },
  "hasSensor": {
    "type": "Relationship",
    "object": "urn:ngsi-ld:Sensor:1234567890"
  },
  "@context": [
    "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    "http://example.org/ngsi-ld/commonTerms.jsonld",
    "http://example.org/ngsi-ld/vehicle.jsonld",
    "http://example.org/ngsi-ld/parking.jsonld"
  ]
}
```

#### Create Entity

To persist the entity in the Neo4j Context Registry database we need to perform a POST to /v2/entities with the ngsild entity as payload. 

##### context resolution

Multiple namespaces are allowed (es. ngsild is the 'core' context, diatomic, sosa and fiware are other namespaces considered). Most of the http requests operations need to specify to wich context is referred. This to prevent Entities NS collisions (es. a Parking entity described both in fiware NS and diatomic NS)



## API queries examples

* Create a new entity

```
http POST http://localhost:8082/ngsi-ld/v1/entities Content-Type:application/json < src/test/kotlin/ngsild/beehive.json Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Get entities by type: a Link header is optionally provided to avoid NS collisions (6.3.5 JSON-LD @context resolution), this Link will be omitted in next examples for clarity but should be provided otherwise the default NS will be considered ngsild

```
http http://localhost:8082/ngsi-ld/v1/entities  type==BeeHive Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Get an entity by URI

```
http http://localhost:8082/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC  Content-Type:application/json
```

* Get entities by relationships

```
http http://localhost:8082/ngsi-ld/v1/entities  type==BeeHive  q==connectsTo==urn:ngsi-ld:Beekeeper:Pascal Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
http http://localhost:8082/ngsi-ld/v1/entities  type==Vehicle  q==isParked==urn:ngsi-ld:OffStreetParking:Downtown1 Link:"<http://easyglobalmarket.com/contexts/example.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Get entities by property

```
http http://localhost:8082/ngsi-ld/v1/entities  type==BeeHive  q==name==ParisBeehive12 Link:"<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Update the property of an entity

```
http PATCH http://localhost:8082/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC/attrs/name Content-Type:application/json < src/resources/ngsild/sensor_update_attribute.json Link:"<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
```

* Update an entity

```
http PATCH http://localhost:8082/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC/attrs  Content-Type:application/json < src/resources/ngsild/sensor_update.json Link:"<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
```
