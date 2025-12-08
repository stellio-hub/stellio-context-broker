# Multitenancy

## Design

In Stellio, each tenant:

* Is defined by a name (which can be any string since 1.7.1)
* Maps to a specific schema in the database
* Binds to a specific realm in Keycloak (if authentication is enabled)

Thus, to add a new tenant in Stellio, you need to configure the aforementioned three properties. As creating a new tenant is not something that is done
every day in a production deployment, and as it implies not easily automatable operations (like creating and configuring a tenant in Keycloak), Stellio
does not currently support on-the-fly creation of tenants. So for a tenant to be taken into account, you need to update the configuration of Stellio then
restart it. 

The detailed behavior is defined in the NGSI-LD specification, section 4.14 - Supporting multiple tenants.

## Declaration

When in development mode, tenants are defined in the `shared.properties` file in the `shared` module.

```
# each tenant maps to a different KC realm (if authentication is enabled) and DB schema
# a tenant declaration is composed of a (tenant) URI, an OIDC issuer URL and a DB schema
application.tenants[0].name = urn:ngsi-ld:tenant:default
application.tenants[0].issuer = https://sso.stellio.io/auth/realms/stellio
application.tenants[0].dbSchema = public
application.tenants[1].name = urn:ngsi-ld:tenant:stellio-dev
application.tenants[1].issuer = https://sso.stellio.io/auth/realms/egm
application.tenants[1].dbSchema = egm
```

Default tenant must always be declared with the `urn:ngsi-ld:tenant:default` value (but, as specified by the NGSI-LD API specification, it does not have to be declared in the HTTP requests and is used if no tenant is specified in a request).

Please also note that, even if authentication is not enabled, you need to specify a value for the OIDC issuer URL property (it will be ignored if authentication is not enabled).

To add a tenant:

* Declare it in the `shared.properties` file (as shown above)
* If authentication is enabled, create and configure the realm in Keycloak
* Restart the context broker
* The DB schema is automatically created when the context broker restarts

When running with the Docker images and using the docker-compose configuration, the tenants are declared in the environment section of the search and subscription services:

```yaml
  search-service:
    environment:
      - APPLICATION_TENANTS_0_NAME=${APPLICATION_TENANTS_0_NAME}
      - APPLICATION_TENANTS_0_ISSUER=${APPLICATION_TENANTS_0_ISSUER}
      - APPLICATION_TENANTS_0_DBSCHEMA=${APPLICATION_TENANTS_0_DBSCHEMA}
```

Where the 3 environment variables are typically declared in the `.env` file:

```shell
APPLICATION_TENANTS_0_NAME=urn:ngsi-ld:tenant:default
APPLICATION_TENANTS_0_ISSUER=https://sso.stellio.io/auth/realms/stellio
APPLICATION_TENANTS_0_DBSCHEMA=public
```

If you want to add a new tenant, simply add the new properties in the `.env` file:

```shell
APPLICATION_TENANTS_1_NAME=openiot
APPLICATION_TENANTS_1_ISSUER=https://sso.stellio.io/auth/realms/openiot
APPLICATION_TENANTS_1_DBSCHEMA=openiot
```

And add the declarations in the environment section of the search and subscription services in the `docker-compose.yml` configuration:

```yaml
  search-service:
    environment:
      - APPLICATION_TENANTS_1_NAME=${APPLICATION_TENANTS_1_NAME}
      - APPLICATION_TENANTS_1_ISSUER=${APPLICATION_TENANTS_1_ISSUER}
      - APPLICATION_TENANTS_1_DBSCHEMA=${APPLICATION_TENANTS_1_DBSCHEMA}
      - [Other environment variables]

  subscription-service:
    environment:
      - APPLICATION_TENANTS_1_NAME=${APPLICATION_TENANTS_1_NAME}
      - APPLICATION_TENANTS_1_ISSUER=${APPLICATION_TENANTS_1_ISSUER}
      - APPLICATION_TENANTS_1_DBSCHEMA=${APPLICATION_TENANTS_1_DBSCHEMA}
      - [Other environment variables]
```

An example configuration for the default tenant can be seen in the docker-compose file available in the GitHub repository of Stellio.

Finally, please note that, if using Stellio with authentication enabled, the associated Keycloak realm must exist when the application starts since the context broker will try to read its configuration during the startup.
