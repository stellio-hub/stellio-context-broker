# Upgrading to 2.0.0

This note describes the necessary steps to upgrade to Stellio 2.0.0

## Introduction

The main change in the 2.0.0 version is the replacement of the neo4j database to a PostgreSQL database (and more specifically, the PG database previously used by the search service is now used by entity and search services). The migration of data is run automatically when the new version starts.

It is also accompanied by a major upgrade of PostreSQL (from version 13 to 14) and TimescaleDB (from version 2.3 to 2.9). Therefore, the migration procedure implies to start with a backup of the current databases and to restore them into the new database.

## Migration procedure

### Prepare the environment

* Define the directory where Stellio docker-compose config is located

```
export STELLIO_COMPOSE_DIR=$HOME/stellio-context-broker
```

* Source the environment

```shell
source $STELLIO_COMPOSE_DIR/.env
```

### Export subjects infos from neo4j

This first step must be done with the currently installed version (since it requires neo4j to be up and running):

* Add a volume in the neo4j service

```
    volume:
        [other volumes]
        - ./subject_export:/var/lib/neo4j/import/subject_export
```

* Add an environement in the neo4j service

```
    environement:
        [other environments]
        - NEO4J_apoc_export_file_enabled=true
```

* Force a restart of neo4j

```
docker compose up -d
```

* Copy the export script into the neo4j container

```
docker cp ./scripts/export_subjects_infos.cypher neo4j:/tmp/.
```

* Run the export script

```
docker exec -it neo4j cypher-shell -u neo4j -p ${NEO4J_PASSWORD} -d stellio -f /tmp/export_subjects_infos.cypher
```

Check in the `subject_export` directory that there are 3 CSV files: export_clients.csv, export_groups.csv and export_users.csv.
Backup PG and neo4j databases

* Stop the Stellio and neo4j services

```
docker compose stop api-gateway entity-service search-service subscription-service neo4j
```

* Backup PG and neo4j databases

```
now=$(date +%Y-%m-%d)

docker exec stellio-postgres /bin/bash -c "export PGPASSWORD=$POSTGRES_PASSWORD && /usr/local/bin/pg_dump -Fc -U postgres stellio_search" | gzip -9 > /tmp/postgres_search_$now.gz
docker exec stellio-postgres /bin/bash -c "export PGPASSWORD=$POSTGRES_PASSWORD && /usr/local/bin/pg_dump -Fc -U postgres stellio_subscription" | gzip -9 > /tmp/postgres_subscription_$now.gz

neo4j_data_volume=$(docker volume inspect --format '{{ .Mountpoint }}' $STELLIO_COMPOSE_DIR_neo4j-storage)
docker run --rm --publish=7474:7474 --publish=7687:7687 --volume=$neo4j_data_volume:/data --volume=/tmp:/backups neo4j:4.4 neo4j-admin dump --database=stellio --to=/backups/neo4j_$now.dump
```

### Switch to new Stellio version

* Stop all the services

```
docker compose stop
```

* Update the version of the Stellio containers in the `.env` file

```
STELLIO_DOCKER_TAG=2.0.0
POSTGRES_PASS={new_password} -- set a new passsword for PG, common for all services
```

### Restore PG databases

* Comment Stellio services in the docker-compose configuration
* Delete PG volume

```
docker container rm stellio-postgres
docker volume rm -f $STELLIO_COMPOSE_DIR_postgres-storage
```

* Download and start the services

```
docker compose pull
docker compose up -d && docker compose logs -f
```

* Copy the dumps into the PG container

```
docker cp /tmp/postgres_search_$now.gz postgres:/tmp/.
docker cp /tmp/postgres_subscription_$now.gz postgres:/tmp/.
```

* Go into the PG container

```
docker exec -it stellio-postgres bash
```

* Go into the PG container and restore the search DB dump

```
now=$(date +%Y-%m-%d)
gunzip /tmp/postgres_search_$now.gz
gunzip /tmp/postgres_subscription_$now.gz
su - postgres
psql stellio_search
SELECT timescaledb_pre_restore();
\! pg_restore -Fc --no-owner --role=stellio -d stellio_search /tmp/postgres_search_{yyyy-MM-dd}
SELECT timescaledb_post_restore();
-- Optional: reindex your database to improve query performance
REINDEX DATABASE stellio_search;
```

* Then restore the subscription DB dump

```
\c stellio_subscription
SELECT timescaledb_pre_restore();
\! pg_restore -Fc --no-owner --role=stellio -d stellio_subscription /tmp/postgres_subscription_{yyyy-MM-dd}
SELECT timescaledb_post_restore();
-- Optional: reindex your database to improve query performance
REINDEX DATABASE stellio_subscription;
```

* Uncomment Stellio services in the docker-compose configuration

```
git reset --hard
```

* Start all the services

```
docker-compose up -d --remove-orphans && docker compose logs -f --tail=100
```

### Import subjects infos into PG

* Copy the subjects infos files exported in the 1st step into the PG container

```
docker cp ./subject_export/export_users.csv postgres:/tmp/.
docker cp ./subject_export/export_groups.csv postgres:/tmp/.
docker cp ./subject_export/export_clients.csv postgres:/tmp/.
```

* Copy the import script into the PG container

```
docker cp ./scripts/import_subjects_infos.sql postgres:/tmp/.
```

* Run the import script

```
docker exec -it stellio-postgres bash -c "su - postgres -c 'psql -d stellio_search -f /tmp/import_subjects_infos.sql'"
```

### Remove unused environment variables and volumes

* Remove the now unused environement variables:

```
NEO4J_PASSWORD
POSTGRES_PASSWORD
STELLIO_SEARCH_DB_PASSWORD
STELLIO_SUBSCRIPTION_DB_PASSWORD
```

* Remove unused neo4j volume

```
docker volume rm -f $STELLIO_COMPOSE_DIR_neo4j-storage
```

* Remove old volume for Kafka

After upgrading to CP-Kafka 7.3.1, the old volume used for data can be removed. However, it can't be done while the Kafka container still exists, so first delete it then remove the old volume:

```
docker volume rm -f $STELLIO_COMPOSE_DIR_kafka-storage
```

## Other significant changes

There is a new improved and cleaned up version of the authorization related endpoints. They are described in the [Authentication and authorization](../user/authentication_and_authorization.md) page.
