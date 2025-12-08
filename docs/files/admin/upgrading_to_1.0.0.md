# Upgrading to 1.0.0

This note describes the necessary steps to upgrade to Stellio 1.0.0

## Upgrade to Timescale 2.3 / PostgreSQL 13

* Prepare the environment variables

```
export POSTGRES_PASSWORD=postgres_user_password
export POSTGRES_CONTAINER_NAME=postgres_container_name (see container_name attribute in service declaration in docker-compose.yml file)
```

* Stop the services using the PostgreSQL databases (to avoid loss of data during the upgrade)

```
docker compose stop search-service subscription-service
```

* Backup the search and subscription databases

```
docker exec $POSTGRES_CONTAINER_NAME /bin/bash -c "export PGPASSWORD=$POSTGRES_PASSWORD && /usr/local/bin/pg_dump -Fc -U postgres stellio_search" | gzip -9 > /tmp/postgres_search.gz
docker exec $POSTGRES_CONTAINER_NAME /bin/bash -c "export PGPASSWORD=$POSTGRES_PASSWORD && /usr/local/bin/pg_dump -Fc -U postgres stellio_subscription" | gzip -9 > /tmp/postgres_subscription.gz
```

* Get the name of the Timescale volume

```
export PG_VOLUME_NAME=$(docker inspect $POSTGRES_CONTAINER_NAME --format='{{range .Mounts }}{{.Name}}{{end}}')
```

* Stop the current running instance

```
docker compose stop $POSTGRES_CONTAINER_NAME
docker container rm $POSTGRES_CONTAINER_NAME
```

* Get and run the Timescale 2.3.0-pg11 image

```
docker pull timescale/timescaledb-postgis:2.3.0-pg11
docker run -v $PG_VOLUME_NAME:/var/lib/postgresql/data -e PGDATA=/var/lib/postgresql/data/pgdata -d --name timescaledb-2.3.0 -p 5432:5432 timescale/timescaledb-postgis:2.3.0-pg11
```

* Upgrade Timescale extension in the two databases

```
docker exec -it timescaledb-2.3.0 psql -U postgres -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it timescaledb-2.3.0 psql -U postgres -X -c "ALTER EXTENSION timescaledb UPDATE;" template1
docker exec -it timescaledb-2.3.0 psql -U postgres -X -c "ALTER EXTENSION timescaledb UPDATE;" stellio_search
docker exec -it timescaledb-2.3.0 psql -U postgres -X -c "ALTER EXTENSION timescaledb UPDATE;" stellio_subscription
```

You can check the extension has been correctly done by running the following command and checking the version column for the timescaledb extension:

```
docker exec -it timescaledb-2.3.0 psql -U postgres -X -c "\dx" stellio_search
```

* Backup again the two databases

```
docker exec timescaledb-2.3.0 /bin/bash -c "export PGPASSWORD=$POSTGRES_PASSWORD && /usr/local/bin/pg_dump -Fc -U postgres stellio_search" | gzip -9 > /tmp/postgres_search_before_pg13.gz
docker exec timescaledb-2.3.0 /bin/bash -c "export PGPASSWORD=$POSTGRES_PASSWORD && /usr/local/bin/pg_dump -Fc -U postgres stellio_subscription" | gzip -9 > /tmp/postgres_subscription_before_pg13.gz
```

* Stop and remove the Timescale container

```
docker stop timescaledb-2.3.0
docker container rm timescaledb-2.3.0
```

* Remove the current data and prepare for the upgrade to PG13

```
docker volume rm $PG_VOLUME_NAME
docker volume create $PG_VOLUME_NAME
```

* Edit `docker-compose.yml` file, change the Timescale image name to `stellio/stellio-timescale-postgis:2.3.0-pg13` and create the new container by running:

```
docker compose up -d
```

Before, be sure to comment out search and subscription services (to avoid that they restart before the end of the migration) and to source any specific environment file).

* Restore the backups made after the update of the Timescale extension

Copy the dumps and connect to the Timescale container:

```
docker cp /tmp/postgres_search_before_pg13.gz $POSTGRES_CONTAINER_NAME:/tmp/.
docker cp /tmp/postgres_subscription_before_pg13.gz $POSTGRES_CONTAINER_NAME:/tmp/.
docker exec -it $POSTGRES_CONTAINER_NAME bash
```

In the container, restore the dumps:

```
gunzip /tmp/postgres_search_before_pg13.gz
gunzip /tmp/postgres_subscription_before_pg13.gz

su - postgres
psql

\c stellio_search
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT timescaledb_pre_restore();
\! pg_restore -Fc -d stellio_search /tmp/postgres_search_before_pg13
SELECT timescaledb_post_restore();

\c stellio_subscription
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT timescaledb_pre_restore();
\! pg_restore -Fc -d stellio_subscription /tmp/postgres_subscription_before_pg13
SELECT timescaledb_post_restore();
```

Exit the container and eventually remove the dumps.

* Edit `docker-compose.yml` file, un-comment search and subscription services and restart the services:

```
docker compose up -d
```

References:

- [Updating TimescaleDB to 2.0](https://docs.timescale.com/timescaledb/latest/how-to-guides/update-timescaledb/update-timescaledb-2)
- [Updating TimescaleDB versions](https://docs.timescale.com/timescaledb/latest/how-to-guides/update-timescaledb)
- [Updating a TimescaleDB Docker installation](https://docs.timescale.com/timescaledb/latest/how-to-guides/update-timescaledb/updating-docker)
- [Logical backups with pg_dump and pg_restore](https://docs.timescale.com/timescaledb/latest/how-to-guides/backup-and-restore/pg-dump-and-restore/#entire-database)

## API breaking changes

### Rename `page` to `offset` in pagination queries
