# Quickstart

This quickstart guide shows a real use case scenario of interaction with the API in an Apiculture context.

## Prepare your environment

The provided examples make use of the HTTPie command line tool (installation instructions: https://httpie.org/docs#installation)

All requests are grouped in a Postman collection that can be found [here](https://www.getpostman.com/collections/a8d09cb6dbf352774eec).

For more details about how to import a Postman collection see https://learning.postman.com/docs/getting-started/importing-and-exporting-data/.    
     
## Provide credentials

- Calls to The API must include an `Authorization` header containing a Bearer access token. It takes the following form:

```
Authorization: Bearer <access token>
```

An access token can be obtained in two ways:

- If a client has its service account enabled, an access token can be obtained with the following request:

```
http --form POST https://sso.eglobalmark.com/auth/realms/stellio/protocol/openid-connect/token client_id=<client_id> client_secret=<client_secret> grant_type=client_credentials
```

- If a client wants to make API calls on behalf of an end user, an access token can be obtained in exchange of the authorization code contained in the redirect URL after a user authenticates on the authentication server. This process, called the Authorization Code Flow, is described exhaustively in the OpenID Connect specification: https://openid.net/specs/openid-connect-core-1_0.html#CodeFlowAuth.

One simple way to have the access token without copy-pasting is to keep it in a variable:

```
export TOKEN=$(http --form POST https://sso.eglobalmark.com/auth/realms/stellio/protocol/openid-connect/token client_id=<client_id> client_secret=<client_secret> grant_type=client_credentials | jq -r .access_token)
```

Then to simply use it in the HTTP requests:

```
http https://data-hub.eglobalmark.com/... Authorization:"Bearer $TOKEN" ...
```

For brevity and clarity, the `Authorization` header is not displayed in the sample HTTP requests described below.

## Case study

This case study is written for anyone who wants to get familiar with the API, we use a real example to make it more concrete.

We will create the following entities:
- A beekeeper
- An apiary
- A beehive
- Sensors linked to the beehive which measures two metrics (temperature and humidity)

* We start by creating the beekeeper, the apiary, the beehive and sensors 
```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/beekeeper.jsonld
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/apiary.jsonld
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/sensor_temperature.jsonld
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/sensor_humidity.jsonld
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities Content-Type:application/ld+json < samples/beehive.jsonld
```

* (optional) We can delete the created entities and recreate them in batch:
   
```
http DELETE https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:Beekeeper:01
http DELETE https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:Apiary:01
http DELETE https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:01
http DELETE https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:02
http DELETE https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01

http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entityOperations/create Content-Type:application/ld+json < samples/apiculture_entities.jsonld
```

* The created beehive can be retrieved by id:

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01
```

* Or by querying all entities of type BeeHive:

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/entities type==BeeHive \
    Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" \
    Content-Type:application/json
```

* Let's add a name to the created beehive: 

```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" \
    Content-Type:application/json \
    < samples/beehive_addName.jsonld
```

* The recently added name property can be deleted: 

```
http DELETE https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs/name \
    Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" \
    Content-Type:application/json
```

* Let's create a subscription to the beehive that sends a notification when the temperature exceeds 40 

```
http POST https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions Content-Type:application/ld+json < samples/subscription_to_beehive.jsonld
```

* The created subscription can be retrieved by id

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:01 Content-Type:application/json
```

* By doing this, increasing the beehive temperature to 42 will raise a notification (the notification is sent via a POST request to the provided URI when creating the subscription, please consider providing working `endpoint` params in order to receive it)

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" \
    Content-Type:application/json \
    < samples/beehive_updateTemperature.jsonld
```

* We can also update the beehive humidity 

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" \
    Content-Type:application/json \
    < samples/beehive_updateHumidity.jsonld
```

* Since we updated both temperature and humidity, we can get the temporal evolution of those properties

```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01 \
    timerel==between \
    time==2019-10-25T12:00:00Z \
    endTime==2020-10-27T12:00:00Z \
    Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" \
    Content-Type:application/json
```

Sample payload returned showing the temporal evolution of temperature and humidity properties:

```json
{
    "@context": [
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld",
        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ],
    "belongs": {
        "createdAt": "2020-06-10T13:27:52.688799Z",
        "modifiedAt": "2020-06-10T13:27:52.695496Z",
        "object": "urn:ngsi-ld:Apiary:01",
        "type": "Relationship"
    },
    "createdAt": "2020-06-10T13:27:52.675092Z",
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
    "managedBy": {
        "createdAt": "2020-06-10T13:27:52.732527Z",
        "modifiedAt": "2020-06-10T13:27:52.741216Z",
        "object": "urn:ngsi-ld:Beekeeper:01",
        "type": "Relationship"
    },
    "modifiedAt": "2020-06-10T13:27:53.111526Z",
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

* Let's try to update again the temperature property of the beehive 

```
http PATCH https://data-hub.eglobalmark.com/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:01/attrs \
    Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" \
    Content-Type:application/json \
    < samples/beehive_secondTemperatureUpdate.jsonld
```

* We can get the simplified temporal representation of the temperature property of the beehive
```
http https://data-hub.eglobalmark.com/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:01?options=temporalValues \
    attrs==temperature \ 
    timerel==between \
    time==2019-10-25T12:00:00Z \
    endTime==2020-10-27T12:00:00Z \
    Link:"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json" \
    Content-Type:application/json 
```

The sample payload returned showing the simplified temporal evolution of temperature and humidity properties:

```json
{
    "@context": [
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld",
        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ],
    "belongs": {
        "createdAt": "2020-06-10T13:27:52.688799Z",
        "modifiedAt": "2020-06-10T13:27:52.695496Z",
        "object": "urn:ngsi-ld:Apiary:01",
        "type": "Relationship"
    },
    "createdAt": "2020-06-10T13:27:52.675092Z",
    "humidity": {
        "createdAt": "2020-06-10T13:27:52.688799Z",
        "modifiedAt": "2020-06-10T13:27:52.695496Z",
        "observedAt": "2019-10-26T21:32:52.98601Z",
        "observedBy": {
            "createdAt": "2020-06-15T11:37:25.75957Z",
            "modifiedAt": "2020-06-15T11:37:25.765321Z",
            "object": "urn:ngsi-ld:Sensor:02",
            "type": "Relationship"
        },
        "type": "Property",
        "unitCode": "P1",
        "value": 60
    },
    "id": "urn:ngsi-ld:BeeHive:01",
    "managedBy": {
        "createdAt": "2020-06-10T13:27:52.732527Z",
        "modifiedAt": "2020-06-10T13:27:52.741216Z",
        "object": "urn:ngsi-ld:Beekeeper:01",
        "type": "Relationship"
    },
    "modifiedAt": "2020-06-10T13:27:53.111526Z",
    "temperature": {
        "createdAt": "2020-06-15T11:37:25.803985Z",
        "modifiedAt": "2020-06-15T11:37:25.83741Z",
        "observedAt": "2019-10-26T21:32:52.98601Z",
        "observedBy": {
            "createdAt": "2020-06-15T11:37:25.830823Z",
            "modifiedAt": "2020-06-15T11:37:25.837429Z",
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
