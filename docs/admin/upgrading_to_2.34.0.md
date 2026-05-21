# Upgrading to 2.34.0

This note describes the necessary steps to upgrade to Stellio 2.34.0

## Upgrade to TimescaleDB 2.26.4

The Timescale extension has been upgraded to 2.26.4. A new [Docker image](https://hub.docker.com/repository/docker/stellio/stellio-timescale-postgis/tags/16-2.26.4-3.6/sha256:4b9c89de24f0bd085983adbe6852e4e0b0d60382e19841ce6d480d3485a4d757) is available on DockerHub.

The general upgrade procedure is described in the [Timescale documentation](https://docs.timescale.com/self-hosted/latest/upgrades/minor-upgrade/) and more specifically in the [Docker upgrade section](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-docker/).

If using the `docker-compose` configuration provided in the Stellio repository, the upgrade can be done in this way (remember to do a backup of the database before starting the upgrade!):

```shell
docker compose pull
docker compose up -d
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
```
