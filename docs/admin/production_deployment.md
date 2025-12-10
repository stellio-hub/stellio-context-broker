# Production deployment

## Compress responses

If transmitting large payloads in response to requests sent to the context broker, it is possible to enable Gzip compression of responses.

For this, the following property has to be passed (and set to `true`) as an environment variable to the corresponding Docker container: `SERVER_COMPRESSION_ENABLED`.

## Version endpoint (since 1.12.0-dev)

It is possible to activate an endpoint that will return the version of Stellio currently in use.

To activate it, add the following configuration in the environment section of the `api-gateway` service in the Docker Compose file:

```yaml
  api-gateway:
    environment:
      - MANAGEMENT_ENDPOINT_INFO_ENABLED=true
```

You call then call the `api-gateway` service at the following path: `/actuator/info` (for instance, `http://localhost:8080/actuator/info` for a local development deployment). The response received looks like:

```
{
    "build": {
        "artifact": "api-gateway",
        "group": "com.egm.stellio",
        "name": "Stellio Context Broker",
        "time": "2021-10-10T11:14:51.675Z",
        "version": "1.12.0-dev"
    }
}
```

Be aware that, once activated, this endpoint will be publicly accessible. If this is not what you want (and it should not be, for a production deployment), be sure to add access restriction to this endpoint (for instance, only allowing a monitoring infrastructure).
