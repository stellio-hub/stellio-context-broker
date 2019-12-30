# Quick start

## API Summary

The API currently exposes the following endpoints:

| Object                                        | Method | Path                                             |
| --------------------------------------------- | ------ | ------------------------------------------------ | 
| Create an entity                              | POST   | /v2/entities                                     |
| Update values of an entity                    | PATCH  | /v2/entities/{entityId}/attrs/                   |
| Add properties and relationships to an entity | POST   | /v2/entities/{entityId}/attrs/ (Soon)            |
| Update value of a property or relationship    | PATCH  | /v2/entities/{entityId}/attrs/{attrId}           |
| Delete value of a property                    | DELETE | /v2/entities/{entityId}/attrs/{attrId} (Soon)    |
| Delete an entity                              | DELETE | /v2/entities/{entityId}                          |
| Search among entities                         | GET    | /v2/entities                                     |
| Retrieve a specific entity                    | GET    | /v2/entities/{entityId}                          |

## NGSI-LD Entity structure

An NGSI-LD entity is serialized in JSON-LD format. The structure has to comply with some requirements amongst them:

- an entity must have an id (represented by `uri:ngsi-ld:<EntityType>:<UUID>`) 
- an entity must have a type
- an entity may have properties
- an entity may have relationships to other entities
- a property may have relationships to other entities
- a relationship may have relationships to other entities

For instance, here is an example of a Vehicle entity :

```json
{
  "id": "urn:ngsi-ld:Vehicle:A1234",
  "type": "Vehicle",
  "brandName": {
    "type": "Property",
    "value": "Tesla"
  },
  "name": "a sample name",
  "isParked": {
    "type": "Relationship",
    "object": "urn:ngsi-ld:OffStreetParking:Downtown2",
    "observedAt": "2019-10-22T12:00:04Z",
    "providedBy": {
      "type": "Relationship",
      "object": "urn:ngsi-ld:Org:Bob"
    }
  },
  "hasSensor": {
    "type": "Relationship",
    "object": "urn:ngsi-ld:Sensor:1234567890"
  },
  "@context": [
      "https://schema.lab.fiware.org/ld/context",
      "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
  ]
}
```

## API usage examples

### Notes on `@context` resolution

Multiple namespaces are allowed (`ngsild` is the mandatory core context, fiware is a frequently used namespace, others are considered).

Most of the HTTP requests need to specify to which contexts they are referring. This to prevent namespaces collisions 
(e.g. a Vehicle entity definition that would exist in two different contexts).

## API queries examples

The provided examples make use of the [HTTPie](https://httpie.org/) command line tool

* Create an entity (with the above Vehicle example)

```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities < vehicle.jsonld
```

* Get an entity by URI

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A1234 Content-Type:application/json
```

* Search entities of type Vehicle

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities type==Vehicle Link:"<https://schema.lab.fiware.org/ld/context>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Search entities of type Vehicle having a given relationship

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities type==Vehicle q==isParked==urn:ngsi-ld:OffStreetParking:Downtown1 Link:"<https://schema.lab.fiware.org/ld/context>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Search entities of type Vehicle having a given property

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities type==Vehicle q==brandName==Tesla Link:"<https://schema.lab.fiware.org/ld/context>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Update a property of an entity

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A1234/attrs/brandName value=Toyota Link:"<https://schema.lab.fiware.org/ld/context>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
```

* Update some properties of an entity

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A1234/attrs brandName=Toyota name=NewName Link:"<https://schema.lab.fiware.org/ld/context>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
```

* Add a relationship to an entity

```
http POST http://localhost:8082/ngsi-ld/v1/entities/urn:ngsi-ld:BreedingService:0214/attrs Content-Type:application/json Link:"<https://gist.githubusercontent.com/bobeal/292f6ddf453bf3c427fb7206a2b5638a/raw/ae9b947caa0761b22a7c8a4078741d52a1f8c651/aquac.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" < src/test/resources/ngsild/aquac/fragments/BreedingService_newRelationshipWithFeeder.json
```
