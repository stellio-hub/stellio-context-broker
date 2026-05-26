# Kafka telemetry ingestion

For high-throughput IoT scenarios, attribute values can be pushed directly to Kafka without going through the HTTP API.
This avoids the overhead of HTTP connection handling and offers an improved resilience.

**Topic**: `cim.telemetry`

**Constraints**:
- The entity must already exist (provisioned once via HTTP).
- Only `Property` values are supported; other attribute types must use the HTTP API.
- No authentication — Kafka is a trusted, internal channel.

**Message format** (the message key must be set to the `entityId`):

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

On receipt, the search-service:
- creates the attribute if it does not exist yet.
- applies merge semantics (NGSI-LD §5.5.12), preserving existing attribute metadata, if it exists.

An `ATTRIBUTE_CREATE` or `ATTRIBUTE_UPDATE` event is then published to `cim.entity._CatchAll` so subscriptions fire as
normal.
