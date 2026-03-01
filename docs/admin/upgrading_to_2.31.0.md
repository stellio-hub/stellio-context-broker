# Upgrading to 2.31.0

This note describes the necessary steps to upgrade to Stellio 2.31.0

## API gateway configuration 

The services URLs are now fully configurable inside the api-gateway. 
As part of this change the `APPLICATION_SEARCH_SERVICE_URL` and `APPLICATION_SUBSCRIPTION_SERVICE_URL`
properties must now contain the entire URL, not only the hostname.

For example:

```
APPLICATION_SEARCH_SERVICE_URL=my-hostname
APPLICATION_SUBSCRIPTION_SERVICE_URL=my-hostname
```

must now be: 

```
APPLICATION_SEARCH_SERVICE_URL=http://my-hostname:8083
APPLICATION_SUBSCRIPTION_SERVICE_URL=http://my-hostname:8084
```

If you don't use one of these variables, the change will not impact you.

## Upgrade to TimescaleDB 2.25.1

The Timescale extension has been upgraded to 2.25.1. A new [Docker image](https://hub.docker.com/repository/docker/stellio/stellio-timescale-postgis/tags/16-2.25.1-3.6/sha256:4016ce82c97d95330906487e4378b872f5910effc48863eace6a72931c38cc14) is available on DockerHub.

The general upgrade procedure is described in the [Timescale documentation](https://docs.timescale.com/self-hosted/latest/upgrades/minor-upgrade/) and more specifically in the [Docker upgrade section](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-docker/).

If using the `docker-compose` configuration provided in the Stellio repository, the upgrade can be done in this way (remember to do a backup of the database before starting the upgrade!):

```shell
docker compose pull
docker compose up -d
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
```
