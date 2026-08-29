# Upgrading to 2.37.0

This note describes the necessary steps to upgrade to Stellio 2.37.0

## Upgrade to PostgreSQL 18 and TimescaleDB 2.29.2

This release moves the Docker image used by the `postgres` service from
`stellio/stellio-timescale-postgis:16-2.26.4-3.6` to `stellio/stellio-timescale-postgis:18-2.29.2-3.6` — a
**PostgreSQL major version upgrade**, combined with a TimescaleDB upgrade.

This is different from the usual TimescaleDB minor upgrades documented in previous release notes
(e.g. [2.31.0](upgrading_to_2.31.0.md)), which only required a `docker compose pull` followed by
`ALTER EXTENSION timescaledb UPDATE;`. A PostgreSQL major version cannot start against data files
created by a previous major version, so it requires a full backup and restore instead. In addition,
`pg_restore` writes directly into TimescaleDB's internal catalog tables, which only works if the
TimescaleDB version is the same on both sides of the restore — so the TimescaleDB upgrade has to
happen first, while still on PostgreSQL 16.

Follow the steps below instead of the [regular backup and restore procedure](backup_and_restore.md) —
they build on it, with the extra steps a PostgreSQL major version upgrade requires. Expect Stellio to
be unavailable for the whole procedure; the downtime scales with the size of your databases, so
consider rehearsing this on a copy of your data first.

### 1 - Stop the Stellio services

Leave Postgres running for now.

```shell
docker compose stop api-gateway search-service subscription-service
```

### 2 - Upgrade the TimescaleDB extension, while still on PostgreSQL 16

Temporarily point the `postgres` service at the `16-2.29.2-3.6` image — same PostgreSQL major version
as today, but the target TimescaleDB version. Edit the `image:` line for the `postgres` service in
`docker-compose-dependencies.yml`:

```diff
-    image: stellio/stellio-timescale-postgis:16-2.26.4-3.6
+    image: stellio/stellio-timescale-postgis:16-2.29.2-3.6
```

Then apply it and upgrade the extension in each database:

```shell
docker compose up -d postgres
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
```

You can check the new version was picked up with:

```shell
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "\dx timescaledb"
```

### 3 - Take a fresh backup

Take the backup now, after the TimescaleDB upgrade above, so it matches the version that will be
restored later. Use the backup script from the [backup and restore documentation](backup_and_restore.md),
or run the equivalent commands directly:

```shell
backup_date=$(date +%Y-%m-%d)
docker exec stellio-postgres /bin/bash -c "su - postgres -c 'pg_dump -Fc stellio_search | gzip -c'" > postgres_search_$backup_date.gz
docker exec stellio-postgres /bin/bash -c "su - postgres -c 'pg_dump -Fc stellio_subscription | gzip -c'" > postgres_subscription_$backup_date.gz
```

### 4 - Update your Stellio checkout to 2.37.0

```shell
git fetch --tags
git checkout 2.37.0
```

This brings `docker-compose-dependencies.yml` to `stellio/stellio-timescale-postgis:18-2.29.2-3.6`.

### 5 - Restore into the new PostgreSQL 18 container

Do **not** reuse the existing data volume — it was created for a different major version.

```shell
docker compose stop postgres
docker volume rm stellio-postgres-storage
docker compose up -d postgres

docker cp postgres_search_$backup_date.gz stellio-postgres:/tmp/.
docker cp postgres_subscription_$backup_date.gz stellio-postgres:/tmp/.
docker exec -it stellio-postgres bash
```

Once in the container:

```shell
backup_date=2026-XX-XX # need to be set again in the container, same date as step 3

gunzip /tmp/postgres_search_$backup_date.gz
gunzip /tmp/postgres_subscription_$backup_date.gz

su - postgres
psql

\c stellio_search
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT timescaledb_pre_restore();
\! pg_restore -Fc -d stellio_search /tmp/postgres_search_2026-XX-XX -- change the date!
SELECT timescaledb_post_restore();

\c stellio_subscription
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT timescaledb_pre_restore();
\! pg_restore -Fc -d stellio_subscription /tmp/postgres_subscription_2026-XX-XX -- change the date!
SELECT timescaledb_post_restore();

exit # from psql
exit # from postgres user and be root again

rm -f /tmp/postgres_search_*
rm -f /tmp/postgres_subscription_*

exit # from the container
```

### 6 - Verify before restarting the rest of Stellio

```shell
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "\dx"
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "SELECT * FROM timescaledb_information.hypertables;"
```

Check that `\dx` reports PostgreSQL 18 / TimescaleDB 2.29.2, and that your hypertables are listed as
expected. If anything looks wrong, stop here and restore your previous setup from the backup taken in
step 3, using the [regular restore procedure](backup_and_restore.md) against the `16-2.26.4-3.6` image.

### 7 - Restart Stellio

```shell
docker compose up -d && docker compose logs -f --tail=100
```

Confirm `search-service` and `subscription-service` start cleanly and that a few NGSI-LD requests
work as expected before considering the upgrade complete.
