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

## Configure the transaction timeout

You can configure the maximum duration of database transactions in `search-service` and `subscription-service` by
setting the `application.transaction-timeout` property. The default timeout is 50 seconds. A zero or negative duration
disables the timeout.

It relies on the PostgreSQL `transaction_timeout` setting (PostgreSQL 17 or above), so it bounds the whole
transaction, not each statement. When it is exceeded, PostgreSQL terminates the connection and Stellio answers with a
`504 Gateway Timeout` error.

When running Stellio with Docker Compose, use `APPLICATION_TRANSACTION_TIMEOUT`:

```dotenv
# Spring duration format: ms, s, m, h, or an ISO-8601 duration
# 50s by default
# Set to 0s or less to disable the timeout
APPLICATION_TRANSACTION_TIMEOUT=50s
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

In `shared.properties`, `application.pagination.temporal-limit` sets the default page size and
`application.pagination.temporal-limit-max` sets the maximum. Both default to 10 000 instances
per attribute and entity. `firstN`/`lastN` above the maximum return `403 TooManyResults`.

For Docker Compose, set these variables in `.env`:

```dotenv
APPLICATION_PAGINATION_TEMPORAL_LIMIT=10000
APPLICATION_PAGINATION_TEMPORAL_LIMIT_MAX=10000
```

## Change the interval of the transient data garbage collection job

Entities and attributes using the transient storage feature (`expiresAt`) are purged by a background job
running by default every day at 4am.

This schedule can be changed by configuring the `search.transient-data.gc.interval` property in `search-service`. It
expects a standard [Spring cron expression](https://docs.spring.io/spring-framework/reference/integration/scheduling.html#scheduling-cron-expression).

If running Stellio from `docker-compose`, it can be configured in the environment section of the `search-service`:

```yaml
    environment:
      - SEARCH_TRANSIENT-DATA_GC_INTERVAL=0 0 * * * *
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

Starting from Stellio 2.11.0, Docker images built for the Stellio services do not have any default memory settings (until version 2.11.0, they had default memory settings for the heap size, but the JVM is able to set [sensible default settings](https://learn.microsoft.com/en-us/azure/developer/java/containers/overview#understand-jvm-default-ergonomics) so they have been removed). The only JVM setting set by default is the entropy source (`-Djava.security.egd=file:/dev/./urandom`, see [Entropy source](performance.md#entropy-source)).

If you want to add some specific JVM settings, you can do so by using the `JDK_JAVA_OPTIONS` environment variable. For instance, if you want to set the heap size memory:

```yaml
    environment:
      - JDK_JAVA_OPTIONS=-Xms1024m -Xmx2048m
```

See [Heap size](performance.md#heap-size) for settings that can improve performance.

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

## Configure the Context Source identity

The Context Source identity returned by `GET /ngsi-ld/v1/info/sourceIdentity` is built from the following properties:

```properties
# alias of this Stellio instance, `stellio` by default
# it must be unique among the federated brokers and be a valid RFC 7230 token (no `:`, `/` or spaces)
# for a tenant other than the default one, the last segment of the tenant name is appended
# (e.g. `stellio-city` for the `urn:ngsi-ld:tenant:city` tenant)
application.context-source-alias = stellio
# optional extras, returned as is in `contextSourceExtras` (the member is omitted if no extra is configured)
application.context-source-extras.deployment = production
application.context-source-extras.contact.email = ops@example.org
```

Some limitations to be aware of:

- extras values are always returned as strings (e.g. `"100"`, not `100`)
- when set via environment variables, map keys are lowercased by Spring Boot
  (`APPLICATION_CONTEXTSOURCEEXTRAS_MAXLIMIT` gives `maxlimit`), use a properties file or `SPRING_APPLICATION_JSON` for
  camelCase keys
- `contextSourceUptime` is the uptime of the search-service instance answering the request, not of the whole deployment
