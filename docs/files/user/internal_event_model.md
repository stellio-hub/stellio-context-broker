# Internal event model

As explained in the [Getting Started page](../quick_start_guide.md), Stellio internally uses a Kafka message broker to decouple communication between the 2 main micro-services.

This communication is based on an event model inspired by the NGSI-LD API and does its best to follow the same design principles.

Currently, the following events are flowing inside the platform:

- Operations done on entities through the NGSI-LD API (including batch operations)
    - Used by the subscription service to trigger notifications if there are some matching subscriptions
- Identity and access management events sent by Keycloak
    - Listened by Stellio services to provision users, groups and clients, as well as their global roles and groups memberships
- Telemetry events received from sensors (for high ingestion rates scenarios)

One core principle of the event model is that an event is atomic, i.e., it contains only one operation. So, for instance, if 2 attributes are added to an entity, 2 events will be propagated.

## Types and structure of events

Here is the list of supported events, accompanied by a sample payload:

- Entity creation

```json
{
    "tenantName": "urn:ngsi-ld:tenant:stellio",
    "entityId": "urn:ngsi-ld:Vehicle:A4567",
    "entityTypes": "Vehicle",
    "operationPayload": "{\"id\": \"urn:ngsi-ld:Vehicle:A4567\", \"type\": \"Vehicle\", \"brandName\": { \"type\": \"Property\", \"value\": \"Mercedes\"}}",
    "contexts": [
        "https://some.host/my-context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.8.jsonld"
    ],
    "operationType": "ENTITY_CREATE"
}
```

- Entity deletion

```json
{
    "tenantName": "urn:ngsi-ld:tenant:stellio",
    "entityId": "urn:ngsi-ld:Vehicle:A4567",
    "entityTypes": ["Vehicle"],
    "contexts": [
        "https://some.host/my-context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.8.jsonld"
    ],
    "operationType": "ENTITY_DELETE"
}
```

- Attribute append

```json
{
    "tenantName": "urn:ngsi-ld:tenant:stellio",
    "entityId": "urn:ngsi-ld:Vehicle:A4567",
    "entityTypes": ["Vehicle"],
    "attributeName": "speed",
    "datasetId": "urn:ngsi-ld:Dataset:GPS",
    "operationPayload": "{ \"value\": 76, \"unitCode\": \"KMH\", \"observedAt\": \"2021-10-26T22:35:52.98601Z\", \"datasetId\": \"urn:ngsi-ld:Dataset:GPS\" }",
    "updatedEntity": "(entity payload after the append operation)",
    "contexts": [
        "https://some.host/my-context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.8.jsonld"
    ],
    "operationType": "ATTRIBUTE_CREATE"
}
```

- Attribute update

```json
{
    "tenantName": "urn:ngsi-ld:tenant:stellio",
    "entityId": "urn:ngsi-ld:Vehicle:A4567",
    "entityTypes": ["Vehicle"],
    "attributeName": "speed",
    "datasetId": "urn:ngsi-ld:Dataset:GPS",
    "operationPayload": "{ \"value\":60, \"observedAt\": \"2021-10-26T23:35:52.98601Z\" }",
    "updatedEntity": "(entity payload after the update operation)",
    "contexts": [
        "https://some.host/my-context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.8.jsonld"
    ],
    "operationType": "ATTRIBUTE_UPDATE"
}
```

- Attribute deletion

```json
{
    "tenantName": "urn:ngsi-ld:tenant:stellio",
    "entityId": "urn:ngsi-ld:Vehicle:A4567",
    "entityTypes": ["Vehicle"],
    "attributeName": "speed",
    "datasetId": "urn:ngsi-ld:Dataset:GPS",
    "updatedEntity": "(entity payload after the delete operation)",
    "contexts": [
        "https://some.host/my-context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.8.jsonld"
    ],
    "operationType": "ATTRIBUTE_DELETE"
}
```

## Mapping from Core API operations to `notificationTrigger` in subscriptions

Starting from version 1.6.1 of the NGSI-LD specification, subscriptions support a new `notificationTrigger` member (see 5.2.12 for more details).
The notification trigger indicates what kind of changes shall trigger a notification.

The following table summarizes the events triggered by each Core API operation:

| Core API operation             | Notification trigger                                                        |
| ------------------------------ | --------------------------------------------------------------------------- |
| Create Entity                  | `entityCreated`<br>one `attributeCreated` event per attribute in the entity |
| Update Attributes              | `attributeCreated` (did non exist previously)<br>`attributeUpdated` (existed previously)<br>`attributeDeleted` (via NGSI-LD Null) |
| Append Attributes              | `attributeCreated` (did non exist previously)<br>`attributeUpdated` (existed previously)|
| Partial Attribute Update       | `attributeUpdated`<br>`attributeDeleted` (via NGSI-LD Null)                 |
| Delete Attribute               | `attributeDeleted`                                                          |
| Delete Entity                  | `entityDeleted`<br>one `attributeDeleted` event per attribute in the entity |
| Merge Entity                   | `attributeCreated` (did non exist previously)<br>`attributeUpdated` (existed previously)<br>`attributeDeleted` (via NGSI-LD Null) |
| Replace Entity                 | `attributeDeleted` (does not exist in new entity)<br>`attributeCreated` (did non exist previously)<br>`attributeUpdated` (exists in previous and new entity) |
| Replace Attribute              | `attributeUpdated`                                                          |
