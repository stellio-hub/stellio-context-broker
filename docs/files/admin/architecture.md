# General architecture

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
    [internal event model](../user/internal_event_model.md) page.

![](images/Stellio_General_Architecture.png)

The services are based on the [Spring Boot](https://spring.io/projects/spring-boot) framework, developed in [Kotlin](https://kotlinlang.org), and built with [Gradle](https://gradle.org).