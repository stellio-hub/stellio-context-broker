# Upgrading to 2.39.0

This note describes the necessary steps to upgrade to Stellio 2.39.0

## Upgrade to Kafka 4.3 (Confluent Platform 8.3.2)

This release moves the Docker image used by the `kafka` service from `confluentinc/cp-kafka:8.1.0` (Apache Kafka 4.1)
to `confluentinc/cp-kafka:8.3.2` (Apache Kafka 4.3).

It is a minor Kafka upgrade: the broker starts directly on an existing data volume, topics, messages and consumer
group offsets are kept, no backup and restore is needed.

### 1 - Update the Kafka image

If you use the `docker-compose-dependencies.yml` file provided by Stellio, it is already up-to-date. Otherwise, edit
the `image:` line of the `kafka` service:

```diff
-    image: confluentinc/cp-kafka:8.1.0
+    image: confluentinc/cp-kafka:8.3.2
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

### 3 - (Optional) Finalize the Kafka upgrade

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
