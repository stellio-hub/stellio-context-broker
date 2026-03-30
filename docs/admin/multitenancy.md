# Multitenancy

## Design

In Stellio, each tenant:

* Is defined by a name (which can be any string since 1.7.1)
* Maps to a specific schema in the database
* Has a specific [authentication configuration](./authentication_integration.md#common-configuration) (if authentication is enabled)

Thus, to add a new tenant in Stellio, you need to configure the aforementioned properties. As creating a new tenant is not something that is done
every day in a production deployment, and as it implies not easily automatable operations like creating an authentication issuer (ex. a tenant in Keycloak), Stellio
does not currently support on-the-fly creation of tenants. So for a tenant to be taken into account, you need to update the configuration of Stellio then
restart it. 

The detailed behavior of tenants is defined in the NGSI-LD specification, section 4.14 - Supporting multiple tenants.

## Declaration

Default tenant must always be declared with the `urn:ngsi-ld:tenant:default` name (but, as specified by the NGSI-LD API specification, it does not have to be declared in the HTTP requests and is used if no tenant is specified in a request).

When running with the Docker images and using the docker-compose configuration, the tenants are declared in the environment section of the search and subscription services:

```yaml
  search-service:
    environment:
      - APPLICATION_TENANTS_0_NAME=${APPLICATION_TENANTS_0_NAME}
      - APPLICATION_TENANTS_0_DBSCHEMA=${APPLICATION_TENANTS_0_DBSCHEMA}
      - APPLICATION_TENANTS_0_ISSUER=${APPLICATION_TENANTS_0_ISSUER}
      - [Other environment variables]

  subscription-service:
    environment:
      - APPLICATION_TENANTS_0_NAME=${APPLICATION_TENANTS_0_NAME}
      - APPLICATION_TENANTS_0_DBSCHEMA=${APPLICATION_TENANTS_0_DBSCHEMA}
      - APPLICATION_TENANTS_0_ISSUER=${APPLICATION_TENANTS_0_ISSUER}
      - APPLICATION_TENANTS_0_CLIENTID=subscription-service
      - APPLICATION_TENANTS_0_CLIENTSECRET=${APPLICATION_TENANTS_0_CLIENTSECRET}
      - [Other environment variables]
```

Where the environment variables are typically declared in the `.env` file:

```shell
APPLICATION_TENANTS_0_NAME=urn:ngsi-ld:tenant:default
APPLICATION_TENANTS_0_DBSCHEMA=public
APPLICATION_TENANTS_0_ISSUER=https://sso.stellio.io/auth/realms/stellio
APPLICATION_TENANTS_0_CLIENTSECRET={subscription-service-client-secret-0}
```

Note: The issuer, client_id and client secret properties are ignored if the authentication is disabled.

If you want to add a new tenant, simply add the new properties in the `.env` file:

```shell
APPLICATION_TENANTS_1_NAME=openiot
APPLICATION_TENANTS_1_DBSCHEMA=openiot
APPLICATION_TENANTS_1_ISSUER=https://sso.stellio.io/auth/realms/openiot
APPLICATION_TENANTS_1_CLIENTSECRET={subscription-service-client-secret-1}

```

And add the declarations in the environment section of the search and subscription services in the `docker-compose.yml` configuration:

```yaml
  search-service:
    environment:
      - APPLICATION_TENANTS_1_NAME=${APPLICATION_TENANTS_1_NAME}
      - APPLICATION_TENANTS_1_DBSCHEMA=${APPLICATION_TENANTS_1_DBSCHEMA}
      - APPLICATION_TENANTS_1_ISSUER=${APPLICATION_TENANTS_1_ISSUER}
      - [Other environment variables]

  subscription-service:
    environment:
      - APPLICATION_TENANTS_1_NAME=${APPLICATION_TENANTS_1_NAME}
      - APPLICATION_TENANTS_1_DBSCHEMA=${APPLICATION_TENANTS_1_DBSCHEMA}
      - APPLICATION_TENANTS_1_ISSUER=${APPLICATION_TENANTS_1_ISSUER}
      - APPLICATION_TENANTS_1_CLIENTID=subscription-service
      - APPLICATION_TENANTS_1_CLIENTSECRET=${APPLICATION_TENANTS_1_CLIENTSECRET}
      - [Other environment variables]
```

An example configuration for the default tenant can be seen in the docker-compose file available in the GitHub repository of Stellio.

Finally, please note that, if using Stellio with authentication enabled, the associated Keycloak realm must exist when the application starts since the context broker will try to read its configuration during the startup.
