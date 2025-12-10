# Upgrading to 2.16.0

This note describes the necessary steps to upgrade to Stellio 2.16.0

## Upgrade to PostgreSQL 16 / Timescale 2.16

### Prepare the environment variables

```sh
export POSTGRES_PASS=postgres_user_password
export POSTGRES_CONTAINER_NAME=postgres_container_name (see container_name attribute in service declaration in docker-compose.yml file)
export PG_VOLUME_NAME=$(docker inspect $POSTGRES_CONTAINER_NAME --format='{{range .Mounts }}{{.Name}}{{end}}')
```

### Step 1: Upgrade to Timescale 2.16.0

* Stop the services

```sh
docker compose stop
```

* Get and run the Stellio Timescale 14-2.16.0-3.3 image

```sh
docker pull stellio/stellio-timescale-postgis:14-2.16.0-3.3
docker run -v $PG_VOLUME_NAME:/var/lib/postgresql -e POSTGRES_USER=stellio -e POSTGRES_PASS=$POSTGRES_PASS -e POSTGRES_DBNAME=stellio_search,stellio_subscription -e POSTGRES_MULTIPLE_EXTENSIONS=postgis,timescaledb,pgcrypto -d --name timescaledb-2.16.0 -p 5432:5432 stellio/stellio-timescale-postgis:14-2.16.0-3.3
```

* Upgrade Timescale extension in the two databases

```sh
docker exec -it timescaledb-2.16.0 bash
su - postgres
psql stellio_search
```

```sql
ALTER EXTENSION timescaledb UPDATE TO '2.16.0';

-- Check 2.16.0 is installed
\dx

-- Fix compressed hypertables with FOREIGN KEY constraints that were created with TimescaleDB versions before 2.15.0
CREATE OR REPLACE FUNCTION pg_temp.constraint_columns(regclass, int2[]) RETURNS text[] AS
$$
  SELECT array_agg(attname) FROM unnest($2) un(attnum) LEFT JOIN pg_attribute att ON att.attrelid=$1 AND att.attnum = un.attnum;
$$ LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

DO $$
DECLARE
  ht_id int;
  ht regclass;
  chunk regclass;
  con_oid oid;
  con_frelid regclass;
  con_name text;
  con_columns text[];
  chunk_id int;

BEGIN

  -- iterate over all hypertables that have foreign key constraints
  FOR ht_id, ht in
    SELECT
      ht.id,
      format('%I.%I',ht.schema_name,ht.table_name)::regclass
    FROM _timescaledb_catalog.hypertable ht
    WHERE
      EXISTS (
        SELECT FROM pg_constraint con
        WHERE
          con.contype='f' AND
          con.conrelid=format('%I.%I',ht.schema_name,ht.table_name)::regclass
      )
  LOOP
    RAISE NOTICE 'Hypertable % has foreign key constraint', ht;

    -- iterate over all foreign key constraints on the hypertable
    -- and check that they are present on every chunk
    FOR con_oid, con_frelid, con_name, con_columns IN
      SELECT con.oid, con.confrelid, con.conname, pg_temp.constraint_columns(con.conrelid,con.conkey)
      FROM pg_constraint con
      WHERE
        con.contype='f' AND
        con.conrelid=ht
    LOOP
      RAISE NOTICE 'Checking constraint % %', con_name, con_columns;
      -- check that the foreign key constraint is present on the chunk

      FOR chunk_id, chunk IN
        SELECT
          ch.id,
          format('%I.%I',ch.schema_name,ch.table_name)::regclass
        FROM _timescaledb_catalog.chunk ch
        WHERE
          ch.hypertable_id=ht_id
      LOOP
        RAISE NOTICE 'Checking chunk %', chunk;
        IF NOT EXISTS (
          SELECT FROM pg_constraint con
          WHERE
            con.contype='f' AND
            con.conrelid=chunk AND
            con.confrelid=con_frelid  AND
            pg_temp.constraint_columns(con.conrelid,con.conkey) = con_columns
        ) THEN
          RAISE WARNING 'Restoring constraint % on chunk %', con_name, chunk;
          PERFORM _timescaledb_functions.constraint_clone(con_oid, chunk);
          INSERT INTO _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name) VALUES (chunk_id, NULL, con_name, con_name);
        END IF;

      END LOOP;
    END LOOP;

  END LOOP;

END
$$;

DROP FUNCTION pg_temp.constraint_columns(regclass, int2[]);

\c stellio_subscription

ALTER EXTENSION timescaledb UPDATE TO '2.16.0';

-- Check 2.16.0 is installed
\dx

-- Fix compressed hypertables with FOREIGN KEY constraints that were created with TimescaleDB versions before 2.15.0
CREATE OR REPLACE FUNCTION pg_temp.constraint_columns(regclass, int2[]) RETURNS text[] AS
$$
  SELECT array_agg(attname) FROM unnest($2) un(attnum) LEFT JOIN pg_attribute att ON att.attrelid=$1 AND att.attnum = un.attnum;
$$ LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

DO $$
DECLARE
  ht_id int;
  ht regclass;
  chunk regclass;
  con_oid oid;
  con_frelid regclass;
  con_name text;
  con_columns text[];
  chunk_id int;

BEGIN

  -- iterate over all hypertables that have foreign key constraints
  FOR ht_id, ht in
    SELECT
      ht.id,
      format('%I.%I',ht.schema_name,ht.table_name)::regclass
    FROM _timescaledb_catalog.hypertable ht
    WHERE
      EXISTS (
        SELECT FROM pg_constraint con
        WHERE
          con.contype='f' AND
          con.conrelid=format('%I.%I',ht.schema_name,ht.table_name)::regclass
      )
  LOOP
    RAISE NOTICE 'Hypertable % has foreign key constraint', ht;

    -- iterate over all foreign key constraints on the hypertable
    -- and check that they are present on every chunk
    FOR con_oid, con_frelid, con_name, con_columns IN
      SELECT con.oid, con.confrelid, con.conname, pg_temp.constraint_columns(con.conrelid,con.conkey)
      FROM pg_constraint con
      WHERE
        con.contype='f' AND
        con.conrelid=ht
    LOOP
      RAISE NOTICE 'Checking constraint % %', con_name, con_columns;
      -- check that the foreign key constraint is present on the chunk

      FOR chunk_id, chunk IN
        SELECT
          ch.id,
          format('%I.%I',ch.schema_name,ch.table_name)::regclass
        FROM _timescaledb_catalog.chunk ch
        WHERE
          ch.hypertable_id=ht_id
      LOOP
        RAISE NOTICE 'Checking chunk %', chunk;
        IF NOT EXISTS (
          SELECT FROM pg_constraint con
          WHERE
            con.contype='f' AND
            con.conrelid=chunk AND
            con.confrelid=con_frelid  AND
            pg_temp.constraint_columns(con.conrelid,con.conkey) = con_columns
        ) THEN
          RAISE WARNING 'Restoring constraint % on chunk %', con_name, chunk;
          PERFORM _timescaledb_functions.constraint_clone(con_oid, chunk);
          INSERT INTO _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name) VALUES (chunk_id, NULL, con_name, con_name);
        END IF;

      END LOOP;
    END LOOP;

  END LOOP;

END
$$;

DROP FUNCTION pg_temp.constraint_columns(regclass, int2[]);
```

* Update the PostgreSQL image name to `stellio/stellio-timescale-postgis:14-2.16.0-3.3` in docker-compose.yml

* Stop and remove the temporary Timescale container

```sh
docker stop timescaledb-2.16.0
docker container rm timescaledb-2.16.0
```

### Upgrade to PostgreSQL 16

* Start only PostgreSQL

```sh
docker compose up -d postgres
```

* Backup the search and subscription databases

```sh
docker exec $POSTGRES_CONTAINER_NAME bash -c "su - postgres -c 'pg_dump -Fc stellio_search | gzip -c'" > /tmp/postgres_search.gz
docker exec $POSTGRES_CONTAINER_NAME bash -c "su - postgres -c 'pg_dump -Fc stellio_subscription | gzip -c'" > /tmp/postgres_subscription.gz
```

* Stop the current running instance

```sh
docker compose stop postgres
docker container rm $POSTGRES_CONTAINER_NAME
```

* Remove the current data and prepare for the upgrade to PG16

```sh
docker volume rm $PG_VOLUME_NAME
```

* Edit `docker-compose.yml` file, change the PG image name to `stellio/stellio-timescale-postgis:16-2.16.0-3.3` and create the new container by running:

```sh
docker compose up -d postgres
```

Be sure to source any specific environment file before restarting the DB.

* Restore the backups made after the update of the Timescale extension

Copy the dumps and connect to the Timescale container:

```sh
docker cp /tmp/postgres_search.gz $POSTGRES_CONTAINER_NAME:/tmp/.
docker cp /tmp/postgres_subscription.gz $POSTGRES_CONTAINER_NAME:/tmp/.
docker exec -it $POSTGRES_CONTAINER_NAME bash
```

In the container, restore the dumps:

```sh
gunzip /tmp/postgres_search.gz
gunzip /tmp/postgres_subscription.gz

su - postgres
psql stellio_search

SELECT timescaledb_pre_restore();
\! pg_restore -Fc -d stellio_search /tmp/postgres_search
SELECT timescaledb_post_restore();


\c stellio_subscription

SELECT timescaledb_pre_restore();
\! pg_restore -Fc -d stellio_subscription /tmp/postgres_subscription
SELECT timescaledb_post_restore();
```

Exit the container and eventually remove the dumps.

References:

- [Upgrades within a Docker container](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-docker/)
- [Upgrade PostgreSQL](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-pg/)

## Upgrade to Confluent Kafka 7.16.0

As Kraft mode is now the default mode in Kafka, the custom startup shell script is no longer necessary and should be removed.

Also, Kafka now requires a `CLUSTER_ID` parameter to be set for each cluster. It can be generated using one the following commands shown in the following page: https://sleeplessbeastie.eu/2021/10/22/how-to-generate-kafka-cluster-id/.

A sample updated Kafka configuration can be seen in the [`docker-compose.yml` file](https://github.com/stellio-hub/stellio-context-broker/blob/develop/docker-compose.yml) provided in the Stellio repository on GitHub.

## Restart all the services

```sh
docker compose pull
docker compose up -d
docker compose logs -f --tail=100
```
