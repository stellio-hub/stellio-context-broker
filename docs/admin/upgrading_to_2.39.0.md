# Upgrading to 2.39.0

This note describes the necessary steps to upgrade to Stellio 2.39.0

## Upgrade of Kafka, TimescaleDB and PostGIS

This release upgrades the Docker images of the two dependencies of Stellio:

- `kafka` service: from `confluentinc/cp-kafka:8.1.0` (Apache Kafka 4.1) to `confluentinc/cp-kafka:8.3.2`
  (Apache Kafka 4.3)
- `postgres` service: from `stellio/stellio-timescale-postgis:18-2.29.2-3.6` to
  [`stellio/stellio-timescale-postgis:18-2.31.0-3.7`](https://hub.docker.com/repository/docker/stellio/stellio-timescale-postgis/tags/18-2.31.0-3.7/sha256-d204a6bda181e7924069532e86288a5a5b33e80420e77c6059a0e3182af7818a)
  (TimescaleDB 2.31.0 and PostGIS 3.7)

Both upgrades are done in a single restart of the platform. For Kafka, it is a minor upgrade: the broker starts
directly on an existing data volume, topics, messages and consumer group offsets are kept. For the database, the
extensions keep running in their previous version until they are explicitly updated (step 3), so Stellio keeps
working in between.

Remember to do a backup of the databases before starting the upgrade!

### 1 - Update the images

If you use the `docker-compose-dependencies.yml` file provided by Stellio, it is already up-to-date. Otherwise, edit
the `image:` lines of the `kafka` and `postgres` services:

```diff
-    image: confluentinc/cp-kafka:8.1.0
+    image: confluentinc/cp-kafka:8.3.2
...
-    image: stellio/stellio-timescale-postgis:18-2.29.2-3.6
+    image: stellio/stellio-timescale-postgis:18-2.31.0-3.7
```

While you are there, you can also remove the `KAFKA_BROKER_ID` environment variable: `broker.id` is deprecated in
recent Kafka versions and `KAFKA_NODE_ID` already sets the identifier of the node.

### 2 - Restart the platform

```shell
docker compose pull
docker compose up -d && docker compose logs -f --tail=100
```

During the restart of Kafka, a few `DUPLICATE_BROKER_REGISTRATION` messages may be logged, they disappear on their own
after some seconds.

### 3 - Update the TimescaleDB and PostGIS extensions

```shell
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION postgis UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION postgis UPDATE;"
```

More details on the TimescaleDB upgrade procedure are available in the
[Timescale documentation](https://www.tigerdata.com/docs/deploy/self-hosted/upgrades/upgrade-docker#upgrade-timescaledb-within-docker).

### 4 - (Optional) Finalize the Kafka upgrade

After the upgrade, the cluster keeps its current metadata version, which still allows to roll back to the `8.1.0`
image. You can check it (shown as `metadata.version`) with:

```shell
docker exec -it stellio-kafka kafka-features --bootstrap-server stellio-kafka:9092 describe
```

Once you are confident with the upgrade, you can move all the features to the Kafka 4.3 level:

```shell
docker exec -it stellio-kafka kafka-features --bootstrap-server stellio-kafka:9092 upgrade --release-version 4.3
```

If your Kafka service sets `KAFKA_OPTS` (e.g. to load a JMX exporter Java agent), the Kafka command line tools inherit
it and fail with `java.net.BindException: Address already in use`. Clear it for the command by adding `-e KAFKA_OPTS=`
after `docker exec -it`.

This operation is irreversible: after it, a rollback to the `8.1.0` image is no longer possible.

Do not postpone this step indefinitely if your cluster reports an old `metadata.version` (e.g. `3.3-IV3`, the oldest
version still supported by Kafka 4.3): a future major version of Kafka will likely drop support for it, and the broker
would then refuse to start until the metadata version is upgraded. Finalize the upgrade before moving to the next major
version of Kafka.

## `q` and `scopeQ` of a Subscription and `q` of a Query are no longer URL-decoded

The `q` and `scopeQ` members of a Subscription, and the `q` member of a Query (`POST /ngsi-ld/v1/entityOperations/query`),
are JSON strings: as mandated by the NGSI-LD specification, percent-encoding only applies to a query that is part of a
URL. Previous versions of Stellio wrongly URL-decoded these members; they are now used as sent.

Subscriptions created with a percent-encoded `q` or `scopeQ` (e.g. `name=="Eiffel%20Tower"`) now look for the literal
`%20` and will no longer send the expected notifications. A `+` in `q` is no longer turned into a space.

As a side effect, the `+` single-level wildcard in the `scopeQ` of a Subscription (e.g. `/Nantes/+`), that was
previously turned into a space, now works as expected.

To find the affected subscriptions, run the following query in the `stellio_subscription` database (once for each
tenant schema if you use multi-tenancy):

```sql
SELECT id, q, scope_q FROM subscription WHERE q ~ '%[0-9A-Fa-f]{2}|\+' OR scope_q ~ '%[0-9A-Fa-f]{2}';
```

Then update them (e.g. with a `PATCH` on the subscription) with a non-encoded value (e.g. `name=="Eiffel Tower"`).
Clients sending a percent-encoded `q` in the body of a Query must be updated the same way.
