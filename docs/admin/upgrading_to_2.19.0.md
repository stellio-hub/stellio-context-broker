# Upgrading to 2.19.0

This note describes the necessary steps to upgrade to Stellio 2.19.0

## Upgrade to TimescaleDB 2.17.2 and PostGIS 3.5

The Timescale extension has been upgraded to 2.17.2 and the PostGIS extension to 3.5. A new [Docker image](https://hub.docker.com/repository/docker/stellio/stellio-timescale-postgis/tags/16-2.17.2-3.5/sha256:0493c3e285f5fc973ac00bb86b5bc9fc4cfee004608702e05cf9ab3715b9079f) is available on DockerHub.

The general upgrade procedure is described in the [Timescale documentation](https://docs.timescale.com/self-hosted/latest/upgrades/minor-upgrade/) and more specifically in the [Docker upgrade section](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-docker/).

If using the `docker-compose` configuration provided in the Stellio repository, the upgrade can be done in this way (do not forget to do a backup of the database before starting the upgrade!):

```shell
docker compose pull
docker compose up -d
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION postgis UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION postgis UPDATE;"
```
