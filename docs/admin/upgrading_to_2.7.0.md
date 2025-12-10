# Upgrading to 2.7.0

This note describes the necessary steps to upgrade to Stellio 2.7.0

## Multitenancy

Starting from version 2.7.0, Stellio supports NGSI-LD Tenants. Please read the [multitenancy page](../user/multitenancy.md) for an explanation of the design and the way to configure and use them.

## Renaming of properties

In the subscription service, two properties have been renamed:

- `application.entity.service-url` has been renamed to `subscription.entity-service-url`
- `application.stellio-url` has been renamed to `subscription.stellio-url`

## Upgrade to TimescaleDB 2.11.1

The Timescale version has been upgraded to 2.11.1. A new [Docker image](https://hub.docker.com/layers/stellio/stellio-timescale-postgis/14-2.11.1-3.3/images/sha256-b80d5a8924f357f6810208c32aa11b6e62484f42aecd70c99e5ebcc3bf9b6b6b?context=repo) is available on DockerHub.

The general upgrade procedure is described in the [Timescale documentation](https://docs.timescale.com/self-hosted/latest/upgrades/minor-upgrade/) and more specifically in the [Docker upgrade section](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-docker/).

If using the `docker-compose` configuration provided in the Stellio repository, the upgrade can be done in this way (do not forget to do a backup of the database before starting the upgrade!):

```shell
docker compose stop
docker compose rm postgres
docker compose pull
docker compose up -d
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker compose restart stellio-postgres stellio-search-service stellio-subscription-service
```
