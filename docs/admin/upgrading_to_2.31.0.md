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
````
If you don't use one of these variables, the change will not impact you.

## Authorization based on JSON Web Token (JWT)

The new authorization system works using only the OIDC token. Meaning you can use Stellio authorization features with any OIDC provider.
You can configure what JWT claims are considered to evaluate access rights with the `application.authentication.claims-paths` environment variable.
```
application.authentication.claims-paths = realm_access.roles,groups_uuids
```
Stellio retrieves all the configured claims in the user token (as well as the user sub) and uses all permissions assigned to one of the claims to evaluate the user rights.

## Migrate the current authorization setup
Out of the box, this lets you assign permission to Keycloak roles instead of groups.
It also means that a desynchronization of Stellio subjects information will no longer impact the NGSI-LD endpoints. (only the subject endpoints)

### Migrate groups permission
> **Warning:** You should follow this migration if you have permissions targeting groups.

Existing permission targeting groups need to access the user groups ids in the token.
For this we have developed a new token mapper which is present in the Keycloak images provided by us starting from [version 26.5.5)(https://hub.docker.com/repository/docker/easyglobalmarket/keycloak/tags/26.5.5/sha256-9746311b62a0300b5834bbea1d10300c0977caf5da22dcb790d75a58f403a6a7).

Once the keycloak image is upgraded, you can configure the token mapper to add the groups uuids in the token.

#### Add the Groups UUID Mapper in Clients scopes > my-scope > Mappers > Add Mapper > by configuration 
(You can use an existing scope or create your own, make sure it is used when generating the token)
![](images/group-uuid-mapper-configuration/step-1.png)

#### Configure the mapper to add the ids behind the `groups_uuids` claim.
![](images/group-uuid-mapper-configuration/step-2.png)

#### Verify that the groups uuids are present in the token. (in Clients > your-client > Clients scopes > evaluate > Generated access token)
![](images/group-uuid-mapper-configuration/step-3.png)

When all the realms used by stellio have the `groups_uuids` claim configured. You are ready to upgrade to version 2.31.0.

## Upgrade to TimescaleDB 2.25.2

The Timescale extension has been upgraded to 2.25.2. A new [Docker image](https://hub.docker.com/repository/docker/stellio/stellio-timescale-postgis/tags/16-2.25.2-3.6/sha256:9b81d52c29542df84eb1dbfeec1f79dd778041466379cb05f63b8ab9a966196c) is available on DockerHub.

The general upgrade procedure is described in the [Timescale documentation](https://docs.timescale.com/self-hosted/latest/upgrades/minor-upgrade/) and more specifically in the [Docker upgrade section](https://docs.timescale.com/self-hosted/latest/upgrades/upgrade-docker/).

If using the `docker-compose` configuration provided in the Stellio repository, the upgrade can be done in this way (remember to do a backup of the database before starting the upgrade!):

```shell
docker compose pull
docker compose up -d
docker exec -it stellio-postgres psql --host=localhost -d stellio_search -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
docker exec -it stellio-postgres psql --host=localhost -d stellio_subscription -U stellio -W -X -c "ALTER EXTENSION timescaledb UPDATE;"
```
