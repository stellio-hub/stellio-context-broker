# Monitoring and alerting

## Monitoring

Stellio services export telemetry data using the [OpenTelemetry](https://opentelemetry.io/) protocol (OTLP).
The recommended backend is an **OpenTelemetry Collector** feeding into the
[Grafana LGTM stack](https://grafana.com/docs/opentelemetry/docker-lgtm/) (Loki for logs,
Tempo for traces, Mimir or Prometheus for metrics), with [Grafana](https://grafana.com/) for visualization.

It is also recommended to monitor the VMs with [node_exporter](https://github.com/prometheus/node_exporter)
and the Docker containers with [cAdvisor](https://github.com/google/cadvisor).

### Enabling OpenTelemetry export

OTel export is controlled by the `otel` Spring profile. It must be included in `SPRING_PROFILES_ACTIVE` for 
the services to emit telemetry data:

```
SPRING_PROFILES_ACTIVE=otel
```

Without this profile, the services start normally but do not send any metrics or logs to the OTel backend.

When using the provided docker-compose setup, this is controlled via the `ENVIRONMENT` variable in the `.env` file:

```
ENVIRONMENT=docker,otel
```

### Metrics

JVM and application metrics are exported via OTLP using Micrometer. The relevant configuration property is
(env var name in parentheses):

- `management.otlp.metrics.export.url` (`MANAGEMENT_OTLP_METRICS_EXPORT_URL`): URL of the OTLP metrics endpoint 
(default: `http://localhost:4318/v1/metrics`)

### Health endpoint

The Stellio services expose a [health endpoint](https://docs.spring.io/spring-boot/docs/current/reference/html/production-ready-features.html#production-ready-health)
at `/actuator/health`. It is enabled by default and does not require the `otel` profile.

An example Prometheus configuration to probe the health of Stellio services
(using [Blackbox exporter](https://github.com/prometheus/blackbox_exporter)):

```yaml
  - job_name: 'Stellio services - health'
    metrics_path: /probe
    scrape_interval: 1m
    params:
      module: [http_200_stellio]
    static_configs:
      - targets: ['http://stellio-host:8080/actuator/health']
        labels:
          name: 'API Gateway'
      - targets: ['http://stellio-host:8083/actuator/health']
        labels:
          name: 'Search Service'
      - targets: ['http://stellio-host:8084/actuator/health']
        labels:
          name: 'Subscription Service'
    relabel_configs:
      - source_labels: [__address__]
        target_label: __param_target
      - source_labels: [__param_target]
        target_label: instance
      - target_label: __address__
        replacement: blackbox_exporter:9115
```

Where `http_200_stellio` is configured in this way:

```yaml
  http_200_stellio:
    prober: http
    timeout: 5s
    http:
      preferred_ip_protocol: "ip4"
      valid_http_versions: ["HTTP/1.1", "HTTP/2.0"]
      valid_status_codes: [200]
      method: GET
      fail_if_body_not_matches_regexp:
        - "UP"
```

Health information can, for instance, be monitored with the following community Grafana dashboard:
[https://grafana.com/grafana/dashboards/12275](https://grafana.com/grafana/dashboards/12275).

It is then easy to create alerts based on the health of the services.

## Logging

The logs produced by the Stellio services are exported via OpenTelemetry (OTLP) and can be ingested by any compatible
backend, such as [Grafana Loki](https://grafana.com/oss/loki/).

Logs are only exported when the `otel` Spring profile is active (see [Enabling OpenTelemetry export](#enabling-opentelemetry-export)).

The following property configures where logs are sent (env var name in parentheses):

- `management.opentelemetry.logging.export.otlp.endpoint` (`MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_ENDPOINT`):
URL of the OTLP logs endpoint (default: `http://localhost:4318/v1/logs`)

### Service identification

Each service sends resource attributes that identify it in the OTel backend. These are pre-configured per service:

- `management.opentelemetry.resource-attributes.service.name`: identifies the service (named as the service, 
e.g., `search-service`)
- `management.opentelemetry.resource-attributes.service.namespace`: identifies the service namespace (always `stellio`)
- `management.opentelemetry.resource-attributes.deployment.environment.name`: identifies the deployment environment;
defaults to `local`, override via the `DEPLOYMENT_ENVIRONMENT_NAME` environment variable

These labels align with the attributes defined by [OpenTemetry](https://opentelemetry.io/docs/specs/semconv/),
making it straightforward to filter logs per service in a Grafana dashboard.

### Docker Compose configuration

In a docker-compose or Docker Swarm based deployment, the environment variables can be declared by adding the following
in the `environment` section of each service:

```yaml
  search-service:
    container_name: search-service
    image: stellio/stellio-search-service:latest-dev
    environment:
      - SPRING_PROFILES_ACTIVE=${ENVIRONMENT}
      - MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_ENDPOINT=${MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_ENDPOINT}
      - MANAGEMENT_OPENTELEMETRY_RESOURCE_ATTRIBUTES_DEPLOYMENT_ENVIRONMENT_NAME=${DEPLOYMENT_ENVIRONMENT_NAME}
      - MANAGEMENT_OTLP_METRICS_EXPORT_URL=${MANAGEMENT_OTLP_METRICS_EXPORT_URL}
```

And in the `.env` file:

```
ENVIRONMENT=docker,otel
DEPLOYMENT_ENVIRONMENT_NAME=production

MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_ENDPOINT=http://otel-collector:4318/v1/logs
MANAGEMENT_OTLP_METRICS_EXPORT_URL=http://otel-collector:4318/v1/metrics
```
