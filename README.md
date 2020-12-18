# Stellio context broker

[![FIWARE Core Context Management](https://nexus.lab.fiware.org/repository/raw/public/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
[![License: Apache-2.0](https://img.shields.io/github/license/stellio-hub/stellio-context-broker.svg)](https://spdx.org/licenses/Apache-2.0.html)
[![Docker badge](https://img.shields.io/docker/pulls/stellio/stellio-entity-service.svg)](https://hub.docker.com/r/stellio)
[![SOF support badge](https://nexus.lab.fiware.org/repository/raw/public/badges/stackoverflow/fiware.svg)](http://stackoverflow.com/questions/tagged/fiware)
[![NGSI-LD badge](https://img.shields.io/badge/NGSI-LD-red.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.01_60/gs_CIM009v010201p.pdf)
<br>
[![Documentation badge](https://readthedocs.org/projects/stellio/badge/?version=latest)](http://stellio.readthedocs.io/en/latest/?badge=latest)
![Release Drafter](https://github.com/stellio-hub/stellio-context-broker/workflows/Release%20Drafter/badge.svg)
![Status](https://nexus.lab.fiware.org/static/badges/statuses/incubating.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=stellio-hub_stellio-context-broker&metric=alert_status)](https://sonarcloud.io/dashboard?id=stellio-hub_stellio-context-broker)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4527/badge)](https://bestpractices.coreinfrastructure.org/projects/4527)

Stellio is an NGSI-LD compliant context broker developed by EGM. NGSI-LD is an Open API and Datamodel specification for 
context management [published by ETSI](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf).

This project is part of [FIWARE](https://www.fiware.org/). For more information check the FIWARE Catalogue entry for
[Core Context](https://github.com/Fiware/catalogue/tree/master/core).

| :books: [Documentation](https://stellio.rtfd.io/) | :whale: [Docker Hub](https://hub.docker.com/orgs/stellio/repositories) |
| ------------------------------------------------- | ---------------------------------------------------------------------- |

## Content

-   [Overview](#overview)
-   [Quick start](#quick-start)
-   [Development](#development)
-   [Usage](#usage)
-   [License](#license)

## Overview

Stellio is composed of 3 business services:
* Entity service is in charge of managing the information context, it is backed by a [neo4j](https://neo4j.com) database
* Search service is in charge of handling the temporal (and geospatial) queries, it is backed by a [TimescaleDB](https://www.timescale.com/) database
* Subscription service is in charge of managing subscriptions and subsequent notifications, it is backed by a [TimescaleDB](https://www.timescale.com/) database

It is completed with:
* An API Gateway module that dispatches requests to downstream services
* A [Kafka](https://kafka.apache.org/) streaming engine that decouples communication inside the broker (and allows plugging other services seamlessly)

The services are based on the [Spring Boot](https://spring.io/projects/spring-boot) framework, developed in [Kotlin](https://kotlinlang.org),
and built with [Gradle](https://gradle.org).

## Quick start

A quick way to start using Stellio is to use the provided `docker-compose.yml` file in the root directory (feel free to change 
the default passwords defined in the `.env` file):

```shell script
docker-compose up -d && docker-compose logs -f
```

It will start all the services composing the Stellio context broker platform and expose them on the following ports:
* API Gateway: 8080
* Entity service: 8082
* Search service: 8083
* Subscription service: 8084

Please note that the environment and scripts are validated on Ubuntu 19.10 and MacOS. Some errors may occur on other platforms.

We also provide an experimental configuration to deploy Stellio in a k8s cluster (only tested in Minikube as of now). For more information, please look at [the README](kubernetes/README.md)

## Development

### Developing on a service

Requirements:
* Java 11 (we recommend using [sdkman!](https://sdkman.io/) to install and manage versions of the JDK)

To develop on a specific service, you can use the provided `docker-compose.yml` file inside each service's directory, for instance:

```shell script
cd entity-service
docker-compose up -d && docker-compose logs -f
```

Then, from the root directory, launch the service:

```shell script
./gradlew entity-service:bootRun
```

### Running the tests

Each service has a suite of unit and integration tests. You can run them without manually launching any external component, thanks
to Spring Boot embedded test support and to the great [TestContainers](https://www.testcontainers.org/) library.

For instance, you can launch the test suite for entity service with the following command:
 
```shell script
./gradlew entity-service:test
```

### Building the project

To build all the services, you can just launch:

```shell script
./gradlew build
```

It will compile the source code, check the code formatting (thanks to [ktLint](https://ktlint.github.io/)) and run the test
suite for all the services.

For each service, a self executable jar is produced in the `build/libs` directory of the service.

If you want to build only one of the services, you can launch:

```shell script
./gradlew entity-service:build
```

### Code quality

Code formatting and standard code quality checks are performed by [ktLint](https://ktlint.github.io/) and 
[Detekt](https://detekt.github.io/detekt/index.html).

ktLint and Detekt checks are automatically performed as part of the build and fail the build if any error is encountered.

If you use IntelliJ:
* You can generate the corresponding ktLint settings with the following command:

```shell script
./gradlew ktlintApplyToIdea
```

* You may consider using a plugin like [Save Actions](https://plugins.jetbrains.com/plugin/7642-save-actions) 
that applies changed code refactoring and optimized imports on a save.

* You can enable Detekt support with the [Detekt plugin](https://github.com/detekt/detekt-intellij-plugin).

### Working locally with Docker images

To work locally with a Docker image of a service without publishing it to Docker Hub, you can follow the below instructions:

* Build a tar image:

```shell script
./gradlew entity-service:jibBuildTar
```

* Load the tar image into Docker:

```shell script
docker load --input entity-service/build/jib-image.tar
```

* Run the image:

```shell script
docker run stellio/stellio-entity-service:latest
```

# Usage

To start using Stellio, you can follow the [API quick start](API_Quick_Start.md).

## License

Stellio is licensed under [APL-2.0](https://spdx.org/licenses/Apache-2.0.html).

It mainly makes use of the following libraries and frameworks (dependencies of dependencies have been omitted):

| Library / Framework |	Licence         |
| ------------------- | --------------- |
| Spring              | APL v2          |
| JSON-LD Java        | BSD-3 Clause    |
| Reactor             | APL v2          |
| Jackson             |	APL v2          |
| JUnit               | EPL v2          |
| Mockk               |	APL v2          |
| JsonPath            |	APL v2          |
| WireMock            | APL v2          |
| Testcontainers      |	MIT             |
| Neo4j OGM           |	APL v2          |

© 2020 EGM
