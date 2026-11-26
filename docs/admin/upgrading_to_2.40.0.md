# Upgrading to 2.40.0

## Harmonisation of temporal pagination

Stellio aligns temporal pagination with NGSI-LD HTTP binding and with the entity pagination. Both
return `200 OK` for successful pages and use the `Link` header to point to available pages. Entity pagination
uses `next` and `prev` links with `limit` and `offset`. Temporal pagination uses `intervalafter` and
`intervalbefore` links with `offsetN` to page through the instances of each attribute.
[ETSI TS 104 176 V1.1.1, clauses 6.4.7.2 and 6.4.7.3](https://www.etsi.org/deliver/etsi_ts/104100_104199/104176/01.01.01_60/ts_104176v010101p.pdf#page=29)

Clients that used `206 Partial Content` and `Content-Range` for temporal pagination should now follow the temporal
links in the `Link` header. See [Consume temporal data](../user/API_walkthrough.md#consume-temporal-data).

### Default temporal page size

`application.pagination.temporal-limit-default` takes precedence over the legacy `application.pagination.temporal-limit`.
Existing deployments can keep their configured page size when upgrading.

If you use the Docker Compose from this repository file, change `APPLICATION_PAGINATION_TEMPORAL_LIMIT` in `.env` to `APPLICATION_PAGINATION_TEMPORAL_LIMIT_DEFAULT`.

If you configure the Stellio process environment directly, use `APPLICATION_PAGINATION_TEMPORAL_LIMIT_DEFAULT`.

See [Temporal pagination configuration](misc_configuration_tips.md#temporal-pagination).
