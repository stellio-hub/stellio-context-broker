# Quick start

## API Summary

The API currently exposes the following endpoints:

| Object                                        | Method | Path                                                     |
| --------------------------------------------- | ------ | -------------------------------------------------------- | 
| Create an entity                              | POST   | /ngsi-ld/v1/entities                                     |
| Search entities                               | GET    | /ngsi-ld/v1/entities                                     |
| Get an entity by id                           | GET    | /ngsi-ld/v1/entities/{entityId}                          |
| Append an attribute to an entity              | POST   | /ngsi-ld/v1/entities/{entityId}/attrs                    |
| Update attributes of an entity                | PATCH  | /ngsi-ld/v1/entities/{entityId}/attrs                    |
| Partial update of an entity attribute         | PATCH  | /ngsi-ld/v1/entities/{entityId}/attrs/{attrId}           |
| Delete an entity                              | DELETE | /ngsi-ld/v1/entities/{entityId}                          |
| Delete an entity attribute                    | DELETE | /ngsi-ld/v1/entities/{entityId}/attrs/{attrId}                          |
| Create a batch of entities                    | POST   | /ngsi-ld/v1/entityOperations/create                      |
| Get the temporal evolution of an entity       | GET    | /ngsi-ld/v1/temporal/entities/{entityId}                 |
| Create a subscription                         | POST   | /ngsi-ld/v1/subscriptions                                |
| Query subscriptions                           | GET    | /ngsi-ld/v1/subscriptions                                |
| Get a subscription by id                      | GET    | /ngsi-ld/v1/subscriptions/{subscriptionId}               |
| Update a subscription                         | PATCH  | /ngsi-ld/v1/subscriptions/{subscriptionId}               |
| Delete a subscription                         | DELETE | /ngsi-ld/v1/subscriptions/{subscriptionId}               |

## NGSI-LD Entity structure

An NGSI-LD entity is serialized in JSON-LD format. The structure has to comply with some requirements amongst them:

- an entity must have an id (represented by `uri:ngsi-ld:<EntityType>:<UUID>`) 
- an entity must have a type
- an attribute denotes a property or a relationship
- an entity may have properties
- an entity may have relationships with other entities
- a property may have properties and relationships with other entities
- a relationship may have properties and relationships with other entities

For instance, beeHive_01.jsonld under the samples directory, represents the NGSI-LD payload of a BeeHive entity having the following NGSI-LD attributes:

- An id: urn:ngsi-ld:BeeHive:01
- A type: BeeHive
- A GeoProperty: Point with coordinates (long=24.30623, lat=60.07966)  
- A Property: Temperature with value 22.2 
- A Property: Humidity with value 60
- A Relationship: Belongs to an NGSI-LD Entity of type Apiary  
- The Temperature Property has a relationship called observedBy with an NGSI-LD Entity of type Sensor responsible for detecting temperature data     
- The Humidity Property has a relationship called observedBy with an NGSI-LD Entity of type Sensor responsible for detecting humidity data
     
## NGSI-LD Subscription structure

An NGSI-LD Subscription is serialized in JSON-LD format. The structure has to comply with some requirements amongst them:

- a subscription must have an id (represented by `uri:ngsi-ld:Subscription:<UUID>`) 
- a subscription must have a type that shall be equal to "Subscription"
- a subscription must have a notification containing the parameters that allow to convey the details of a notification (details in the following example)
- a subscription may have other attributes: name, description, entities, q (query), geoQ (geo query) ...

For instance, subscription_to_beeHive.jsonld under the samples directory, represents the NGSI-LD payload of a Subscription to the previous BeeHive Entity that sends a notification when the temperature exceeds 40.

Note: The `endpoint.info` field contains optional information that may be needed when contacting the notification endpoint, the key/value pairs are added in the header of the HTTP POST request. For instance this could be Authorization headers in case of HTTP binding of the API. 

## Authentication

The API is protected by an OpenID Connect compliant authentication server (precisely a [Keycloak](https://www.keycloak.org) server).

Thus, any call made to the API must include an `Authorization` header containing a Bearer access token. It takes the following form:

```
Authorization: Bearer <access token>
```

An access token can be obtained in two ways:

- If a client has its service account enabled, an access token can be obtained with the following request:

```
http --form POST https://data-hub.eglobalmark.com/auth/realms/datahub/protocol/openid-connect/token client_id=<client_id> client_secret=<client_secret> grant_type=client_credentials
```

- If a client wants to make API calls on behalf of an end user, an access token can be obtained in exchange of the authorization code contained in the redirect URL after an user authenticates on the authentication server. This process, called the Authorization Code Flow, is described exhaustively in the OpenID Connect specification: https://openid.net/specs/openid-connect-core-1_0.html#CodeFlowAuth.

One simple way to have the access token without copy-pasting is to keep it in a variable:

```
export TOKEN=$(http --form POST https://data-hub.eglobalmark.com/auth/realms/datahub/protocol/openid-connect/token client_id=<client_id> client_secret=<client_secret> grant_type=client_credentials | jq -r .access_token)
```

Then to simply use it the HTTP requests:

```
http https://data-hub.eglobalmark.com/... Authorization:"Bearer $TOKEN" ...
```

For brevity and clarity, the `Authorization` header is not displayed in the sample HTTP requests described below.

## Notes on `@context` resolution

Multiple namespaces are allowed (`ngsild` is the mandatory core context, fiware is a frequently used namespace, others are defined in this project's sub-directories).

Most of the HTTP requests need to specify the contexts they are referring to, in order to prevent namespaces collisions (e.g. a Vehicle entity definition that would exist in two different contexts).

## API queries examples

The provided examples make use of the [HTTPie](https://httpie.org/) command line tool

* Create an entity (with the above BeeHive example)

```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities < samples/beeHive_01.jsonld
```

* Get an entity by URI

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01 Content-Type:application/json
```

* Search BeeHive entities

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities type==BeeHive Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Search BeeHive entities belongs to ApiarySophia

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities type==BeeHive q==belongs==urn:ngsi-ld:Apiary:XYZ01 Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Search BeeHive entities whose temperature is above 25

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities type==BeeHive q==temperature>20 Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Partial update of the property of an entity

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/humidity value=30 Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
```

* Update some properties of an entity

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" < samples/beehive_01_new_temperature.jsonld
```

* Add a property to an entity

```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs Content-Type:application/json Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" < samples/beeHive_01_addName.jsonld
```

* Get the temporal evolution of a property

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 timerel==between time==2020-02-01T12:00:00Z endTime==2020-02-10T12:00:00Z Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* Create a subscription (with the above Subscription example)

```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions < samples/subscription_to_beeHive.jsonld
```

* Query subscriptions

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions Content-Type:application/json
```

* Get a subscription by URI

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Content-Type:application/json
```

* Update a subscription

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Content-Type:application/json < samples/subscription__01_newQuery.json
```

* Update a subscription Endpoint

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Content-Type:application/json < samples/subscription__01_newEndpoint.json
```

* Delete a subscription

```
http DELETE https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Content-Type:application/json
```

## API Use case

This part presents a use case of interaction with the API following a real scenario.

* We start by creating Sensors, an Apiary and a BeeHive 
```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities < samples/sensor_temperature.jsonld
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities < samples/sensor_humidity.jsonld
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities < samples/apiary_01.jsonld
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities < samples/beeHive_01.jsonld
```

* The entities can also be created in batch:
   
```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entityOperations/create < samples/apiculture_entities.jsonld
```

* The created beeHive can be retrieved by id:

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01
```

* Or by querying all entities of type beeHive:

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities type==BeeHive Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

* We can add a name to the created beeHive: 

```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs Content-Type:application/json Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" < samples/beeHive_01_addName.jsonld
```

* The recently added name property can be deleted: 

```
http DELETE https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/name Content-Type:application/json Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
```

* We create a subscription to the beeHive that sends a notification when the temperature exceeds 40 

```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions < samples/subscription_to_beeHive.jsonld
```

* The created subscription can be retrieved by id

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Content-Type:application/json
```

* Increasing the beeHive temperature to 60 will raise a notification 

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" < samples/beeHive_01_updateTemperature.jsonld
```

* Get the temporal evolution of the beeHive properties

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 timerel==between time==2020-02-01T12:00:00Z endTime==2020-02-10T12:00:00Z Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" Content-Type:application/json
```

Other resources:

- Reference specification of the NGSI-LD API: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.01_60/gs_CIM009v010201p.pdf
- NGSI-LD FAQ by FIWARE Foundation: https://fiware-datamodels.readthedocs.io/en/latest/ngsi-ld_faq/index.html
