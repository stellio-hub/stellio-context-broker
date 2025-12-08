# Upgrading to 2.24.0

This note describes the necessary steps to upgrade to Stellio 2.24.0

## Upgrade to TimescaleDB 2.20.2

The Timescale extension has been upgraded to 2.20.2. A new [Docker image](https://hub.docker.com/repository/docker/stellio/stellio-timescale-postgis/tags/16-2.20.2-3.5/sha256-bb0ee7351c557ad8c0f2a27ec6ff6daac049675868741f3e96706456f098d30b) is available on DockerHub.

The general upgrade procedure is described in the [Timescale documentation](https://docs.timescale.com/self-hosted/latest/upgrades/minor-upgrade/) and more specifically in the [Docker upgrade section](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-docker/).

If using the `docker-compose` configuration provided in the Stellio repository, the upgrade can be done in this way (do not forget to do a backup of the database before starting the upgrade!):

```shell
docker compose pull
docker compose up -d
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
```

Also, starting from this version, there is a new documentation available explaining [how to enable compression in TimescaleDB](./enabling_timescale_compression.md).