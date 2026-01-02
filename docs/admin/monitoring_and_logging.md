# Monitoring and alerting

## Monitoring

The [Prometheus](https://prometheus.io/) platform is typically used to monitor Stellio in production, together with [Grafana]("https://grafana.com/") for the visualisation of the different metrics gathered from the platform.

Setting up of Prometheus is of course beyond the scope of this documentation, there is good documentation on the Prometheus site.

However, it is recommended to monitor the VMs with [node_exporter]("https://github.com/prometheus/node_exporter") and the Docker containers with [cAdvisor]("https://github.com/google/cadvisor").

The Stellio services can also be configured to expose an [health endpoint]("https://docs.spring.io/spring-boot/docs/current/reference/html/production-ready-features.html#production-ready-health") and [Prometheus metrics]("https://docs.spring.io/spring-boot/docs/current/reference/html/production-ready-features.html#production-ready-metrics-export-prometheus") (in parenthesis, the name of the environement variable to use when injecting the values into a Docker container):

- `management.endpoint.prometheus.enabled` (`MANAGEMENT_ENDPOINT_PROMETHEUS_ENABLED`): `true`
- `management.endpoints.web.exposure.include` (`MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE`): `health,prometheus`
- `management.metrics.pf.tag` (`MANAGEMENT_METRICS_PF_TAG`): used to compose a specific `application` tag that is used for easier querying in Prometheus

In a docker-compose or Docker Swarm based deployment, the environement variables can be declared by adding the following in the `environment` section:

```
  search-service:
    container_name: search-service
    image: stellio/stellio-search-service:latest-dev
    environment:
      - MANAGEMENT_ENDPOINT_PROMETHEUS_ENABLED=${MANAGEMENT_ENDPOINT_PROMETHEUS_ENABLED}
      - MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE=${MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE}
      - MANAGEMENT_METRICS_TAGS_APPLICATION=Search Service - ${MANAGEMENT_METRICS_PF_TAG}
```

### Example Prometheus configurations

An example Prometheus configuration to get health information from Stellio services (using [Blackbox exporter]("https://github.com/prometheus/blackbox_exporter")):

```
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

```
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

Health information can for instance be then monitored with the following community Grafana dashboard: [https://grafana.com/grafana/dashboards/12275](https://grafana.com/grafana/dashboards/12275).

An example Prometheus configuration to get metrics information from Stellio services:

```
  - job_name: 'Stellio services - metrics'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 30s
    static_configs:
      - targets: ['stellio-host:8083'] # 8083 : Search service
      - targets: ['stellio-host:8084'] # 8084 : Subscription service
```

Metrics can for instance be then viewed with the following community Grafana dashboard: [https://grafana.com/grafana/dashboards/4701](https://grafana.com/grafana/dashboards/4701).

### Alerting

The Prometheus alert manager can be used to monitor the activity and send alerts in case something is going wrong.

A good place to find some example alerts is the [Awesome Prometheus alerts]("https://awesome-prometheus-alerts.grep.to/") site.

Some classic alerts that are generally recommended:

- Alert when a service is down

```
- name: service
  rules:
  - alert: service_down
    expr: probe_success == 0
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "Service {{ $labels.name }} is down"
      description: "Service {{ $labels.name }} is down ({{ $labels.instance }})"
```

- Alert when a container is down

```
  - alert: container_down_stellio
    expr: |
          absent(container_memory_usage_bytes{name="api-gateway",job="Stellio Docker"}) or
          absent(container_memory_usage_bytes{name="postgres",job="Stellio Docker"}) or
          absent(container_memory_usage_bytes{name="kafka",job="Stellio Docker"}) or
          absent(container_memory_usage_bytes{name="subscription-service",job="Stellio Docker"}) or
          absent(container_memory_usage_bytes{name="search-service",job="Stellio Docker"})
    for: 30s
    labels:
      severity: critical
    annotations:
      summary: "Container {{ $labels.name }} is down on Stellio Dev"
      description: "Container {{ $labels.name }} is down for more than 30 seconds on Stellio Dev"
```

## Logging

The logs produced by the Stellio services can be sent to [Graylog]("https://www.graylog.org/") or any other GELF compatible logging platform.

Setting up of Graylog is of course beyond the scope of this documentation, there is good documentation on the Graylog site.

In order to send Stellio services logs to the logging platform, the following three variables have to configured (in parenthesis, the name of the environement variable to use when injecting the values into a Docker container):

- `application.graylog.host` (`APPLICATION_GRAYLOG_HOST`): host where Graylog is installed (e.g., localhost)
- `application.graylog.port` (`APPLICATION_GRAYLOG_PORT`): port where Graylog is listening (e.g.g, 12201)
- `application.graylog.source` (`APPLICATION_GRAYLOG_SOURCE`): sent as `platform` key to Graylog (it can later be used to create streams specific to the originating platform)

In a docker-compose or Docker Swarm based deployment, the environement variables can be declared by adding the following in the `environment` section:

```
  entity-service:
    container_name: entity-service
    image: stellio/stellio-entity-service:latest
    environment:
      - APPLICATION_GRAYLOG_HOST=${APPLICATION_GRAYLOG_HOST}
      - APPLICATION_GRAYLOG_PORT=${APPLICATION_GRAYLOG_PORT}
      - APPLICATION_GRAYLOG_SOURCE=${APPLICATION_GRAYLOG_SOURCE}
```