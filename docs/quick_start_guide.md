# Overview

## General architecture

Stellio is composed of 2 business services:

-   One service (called `search-service` in the source) is in charge of handling the requests 
    for the Core and Temporal APIs. It is backed by a PostgreSQL database, extended with 
    the [TimescaleDB](https://www.timescale.com/) and [PostGIS](https://postgis.net/) extensions 
-   One service (called `subscription-service` in the source) is in charge of handling the requests 
    for the Subscription API. It is backed by a PostgreSQL database, extended with the
    [PostGIS](https://postgis.net/) extension

It is completed with:

-   An API Gateway component, based on [Spring Cloud Gateway](https://cloud.spring.io/spring-cloud-gateway/reference/html/), 
    that simply forwards the incoming requests to one of the downstream services, based on the request path
-   A [Kafka](https://kafka.apache.org/) streaming engine that decouples communication inside the broker 
    (and allows plugging other services seamlessly). It is described in more details in the
    [internal event model](user/internal_event_model.md) page.

![](admin/images/Stellio_General_Architecture.png)

The services are based on the [Spring Boot](https://spring.io/projects/spring-boot) framework, developed in [Kotlin](https://kotlinlang.org), and built with [Gradle](https://gradle.org).

# Quick start

A quick way to start using Stellio is to use the provided `docker-compose.yml` file in the root directory 
(feel free to change the default passwords defined in the `.env` file):

```shell
docker compose up -d && docker compose logs -f
```

It will start all the services composing the Stellio context broker platform and expose them on the following ports:

-   API Gateway: 8080
-   Search service: 8083
-   Subscription service: 8084

Please note that the environment and scripts are validated on recent Debian/Ubuntu versions and MacOS. Some errors may occur on other platforms.

Docker images are available on [Docker Hub](https://hub.docker.com/orgs/stellio/repositories).

# Usage

To start using Stellio, you can follow the [API quick guide](API_walkthrough.md).

As the development environment does not make use of the authentication setup, you can ignore related information in the API quick guide.
