# Misc configuration tips

## Increase the max allowed size for headers

If sending HTTP requests with headers having a large size, you may have to increase the max allowed size (which is by default set to 8KB).

It may be done by configuring the `server.max-http-request-header-size` property. It has to be done both in api-gateway and in search-service (because both are running an HTTP server).

If running Stellio from `docker-compose`, it can be configured in the environment section of both services:

```yaml
    environment:
      - SERVER_MAX-HTTP-REQUEST-HEADER-SIZE=100KB
```

If running Stellio in Kubernetes, it can be configured in the deployments:

```yaml
- name: SERVER_MAX_HTTP_REQUEST_HEADER_SIZE
  value: 512KB
```

## Increase the max allowed size for body

If sending HTTP requests with a body having a large size, you may have to increase the max allowed size (which is by default set to 2Mb).

It may be done by configuring the `spring.http.codecs.max-in-memory-size` property (see https://docs.spring.io/spring-boot/appendix/application-properties/index.html#application-properties.web.spring.http.codecs.max-in-memory-size). It has to be done in search-service.

If running Stellio from `docker-compose`, it can be configured in the environment section of the service:

```yaml
    environment:
      # Allow 10Mb payloads
      - SPRING_HTTP_CODECS_MAX-IN-MEMORY-SIZE=10485760
```

## Increase the default and maximum limit for pagination

Stellio has a default pagination limit of 30 and a maximum of 100. 

These values can be changed by configuring the `application.pagination.limit-default` and `application.pagination.limit-max` properties in `shared.properties` file.

If running Stellio from `docker-compose`, it can be configured in the environment section of the target service (`search-service` or `subscription-service`) : 

```
      - APPLICATION_PAGINATION_LIMIT-DEFAULT=3O
      - APPLICATION_PAGINATION_LIMIT-MAX=100
```

### Temporal pagination
There is also a default limit for temporal pagination set at 10 000.

you can change it via `application.pagination.temporal-limit` in shared.properties.

Or by adding the variable in the environment target service  (`search-service`)
```
    - APPLICATION_PAGINATION_TEMPORAL-LIMIT=9999
```

## Change the log level of a library / namespace

Add a new environment for the target namespace. For instance, adding:

```yaml
    environment:
      - LOGGING_LEVEL_IO_R2DBC_POSTGRESQL_QUERY=DEBUG
```

Will set the DEBUG level for the `io.r2dbc.postgresql.QUERY` namespace.

For this change to take effet, the Docker container has to be re-created.

## Change the JVM parameters in a container

Starting from Stellio 2.11.0, Docker images built for the Stellio services do not have any default JVM settings (until version 2.11.0, they had default memory settings for the heap size, but the JVM is able to set [sensible default settings](https://learn.microsoft.com/en-us/azure/developer/java/containers/overview#understand-jvm-default-ergonomics) so they have been removed).

If you want to add some specific JVM settings, you can do so by using the `JDK_JAVA_OPTIONS` environment variable. For instance, if you want to set the heap size memory:

```yaml
    environment:
      - JDK_JAVA_OPTIONS=-Xms1024m -Xmx2048m
```

## Launch Stellio with a different port binding
You can launch Stellio with a different port binding using environment variables:

- api-gateway: 
  - API_GATEWAY_PORT=8090
- search-service: 
  - SEARCH_SERVICE_PORT=8093
  - SUBSCRIPTION_ENTITY_SERVICE_URL=http://search-service:8093
- subscription-service: 
  - SUBSCRIPTION_SERVICE_PORT=8094
  - SUBSCRIPTION_STELLIO_URL=http://localhost:8090
- postgres: 
  - POSTGRES_PORT=5433
- kafka:
  - KAFKA_PORT=29093