# Upgrading to 2.29.0

This note describes the necessary steps to upgrade to Stellio 2.29.0

## Upgrade to TimescaleDB 2.24.0

The Timescale extension has been upgraded to 2.24.0. A new [Docker image](https://hub.docker.com/repository/docker/stellio/stellio-timescale-postgis/tags/16-2.24.0-3.6/sha256-1368e8a867b0514f8a0694569a4d3ad0cac7ca20c159f05a1710cc9066d8c1f0) is available on DockerHub.

The general upgrade procedure is described in the [Timescale documentation](https://docs.timescale.com/self-hosted/latest/upgrades/minor-upgrade/) and more specifically in the [Docker upgrade section](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-docker/).

If using the `docker-compose` configuration provided in the Stellio repository, the upgrade can be done in this way (remember to do a backup of the database before starting the upgrade!):

```shell
docker compose pull
docker compose up -d
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
```
