# Kafka telemetry ingestion

For high-throughput IoT scenarios, attribute values can be pushed directly to Kafka without going through the HTTP API.
This avoids the overhead of HTTP connection handling and offers an improved resilience.

**Topic**: `cim.telemetry`

**Constraints**:

- The entity must already exist (provisioned once via HTTP).
- `Property` and `GeoProperty` attribute types are supported; other attribute types must use the HTTP API.
- No authentication — Kafka is a trusted, internal channel.

**Message format** (the message key must be set to the `entityId`):

Property:
```json
{
  "tenantName": "urn:ngsi-ld:tenant:stellio",
  "entityId": "urn:ngsi-ld:Vehicle:A4567",
  "attributeName": "https://vocab.egm.io/speed",
  "datasetId": "urn:ngsi-ld:Dataset:GPS",
  "value": 25.3,
  "observedAt": "2026-01-01T00:00:00Z"
}
```

GeoProperty:
```json
{
  "tenantName": "urn:ngsi-ld:tenant:stellio",
  "entityId": "urn:ngsi-ld:Vehicle:A4567",
  "attributeName": "https://uri.etsi.org/ngsi-ld/location",
  "attributeType": "https://uri.etsi.org/ngsi-ld/GeoProperty",
  "value": {
    "type": "Point",
    "coordinates": [24.30623, 60.07966]
  },
  "observedAt": "2026-01-01T00:00:00Z"
}
```

The `attributeType` field defaults to `https://uri.etsi.org/ngsi-ld/Property` when omitted. For a `GeoProperty`, it must be set explicitly and the `value` must be a valid GeoJSON geometry object (any GeoJSON geometry except GeometryCollection).

On receipt, the search-service:

- creates the attribute if it does not exist yet.
- applies merge semantics (NGSI-LD TS 104 175 §8.4.3), preserving existing attribute metadata, if it exists.

An `ATTRIBUTE_CREATE` or `ATTRIBUTE_UPDATE` event is then published to `cim.entity._CatchAll` so subscriptions fire as
normal.
