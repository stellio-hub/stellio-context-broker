# Upgrading to 2.39.0

This note describes the necessary steps to upgrade to Stellio 2.39.0

## Upgrade to TimescaleDB 2.30.2

The Timescale extension has been upgraded to 2.30.2. A new [Docker image](https://hub.docker.com/repository/docker/stellio/stellio-timescale-postgis/tags/18-2.30.2-3.6/sha256-bb05e6072db8bed8457e73daaacd0d11e46cc4d10ee9a50183d51cd67092bc09) is available on DockerHub.

The general upgrade procedure is described in the [Timescale documentation](https://www.tigerdata.com/docs/deploy/self-hosted/upgrades) and more specifically in the [Docker upgrade section](https://www.tigerdata.com/docs/deploy/self-hosted/upgrades/upgrade-docker#upgrade-timescaledb-within-docker).

If using the `docker-compose` configuration provided in the Stellio repository, the upgrade can be done in this way (remember to do a backup of the database before starting the upgrade!):

```shell
docker compose pull
docker compose up -d
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
```
