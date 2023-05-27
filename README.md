# Stellio context broker

[![FIWARE Core Context Management](https://nexus.lab.fiware.org/repository/raw/public/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
![Status](https://nexus.lab.fiware.org/static/badges/statuses/full.svg)
[![NGSI-LD badge](https://img.shields.io/badge/NGSI-LD-red.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.06.01_60/gs_CIM009v010601p.pdf)
[![SOF support badge](https://nexus.lab.fiware.org/repository/raw/public/badges/stackoverflow/fiware.svg)](http://stackoverflow.com/questions/tagged/fiware)
<br>
[![Documentation badge](https://readthedocs.org/projects/stellio/badge/?version=latest)](http://stellio.readthedocs.io/en/latest/?badge=latest)
[![Docker badge](https://img.shields.io/docker/pulls/stellio/stellio-entity-service.svg)](https://hub.docker.com/r/stellio)
[![License: Apache-2.0](https://img.shields.io/github/license/stellio-hub/stellio-context-broker.svg)](https://spdx.org/licenses/Apache-2.0.html)
<br>
![Build](https://github.com/stellio-hub/stellio-context-broker/workflows/Build/badge.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=stellio-hub_stellio-context-broker&metric=alert_status)](https://sonarcloud.io/dashboard?id=stellio-hub_stellio-context-broker)
[![CodeQL](https://github.com/stellio-hub/stellio-context-broker/actions/workflows/codeql.yml/badge.svg)](https://github.com/stellio-hub/stellio-context-broker/actions/workflows/codeql.yml)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4527/badge)](https://bestpractices.coreinfrastructure.org/projects/4527)

Stellio is an NGSI-LD compliant context broker developed by EGM. NGSI-LD is an Open API and Datamodel specification for 
context management [published by ETSI](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf).

Stellio is a [FIWARE](https://www.fiware.org/) Generic Enabler. Therefore, it can be integrated as part of any platform “Powered by FIWARE”. 
FIWARE is a curated framework of open source platform components which can be assembled together with other third-party 
platform components to accelerate the development of Smart Solutions. For more information check the FIWARE Catalogue entry for
[Core Context](https://github.com/Fiware/catalogue/tree/master/core). The roadmap of this FIWARE GE is described [here](./docs/roadmap.md).

You can find more info at the [FIWARE developers](https://developers.fiware.org/) website and the [FIWARE](https://fiware.org/) website.
The complete list of FIWARE GEs and Incubated FIWARE GEs can be found in the [FIWARE Catalogue](https://catalogue.fiware.org/).

##### NGSI-LD Context Broker Feature Comparison

The NGSI-LD Specification is regularly updated published by ETSI. The latest specification is [version 1.6.1 ](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.06.01_60/gs_CIM009v010601p.pdf) which was  published in **September 2022**. 

-  An Excel file detailing the current compatibility of the  development version of the Stellio Context Broker against the features of the 1.6.1 specification can be downloaded [here](https://docs.google.com/spreadsheets/d/e/2PACX-1vRxOjsDf3lqhwuypJ---pZN2OlqFRl0jyoTV0ewQ1WFnpe7xQary3uxRjunbgJkwQ/pub?output=xlsx)

| :books: [Documentation](https://stellio.rtfd.io/) | :whale: [Docker Hub](https://hub.docker.com/orgs/stellio/repositories) | :dart: [Roadmap](./docs/roadmap.md) |
|---------------------------------------------------|------------------------------------------------------------------------|-------------------------------------|

## Overview

Stellio is composed of 2 business services:
* Search service is in charge of managing the information context and handling the temporal (and geospatial) queries, it is backed by a [TimescaleDB](https://www.timescale.com/) database
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
* Search service: 8083
* Subscription service: 8084

Please note that the environment and scripts are validated on Ubuntu and macOS. Some errors may occur on other platforms.

We also provide an experimental configuration to deploy Stellio in a k8s cluster (only tested in Minikube as of now). For more information, please look at [the README](kubernetes/README.md)

## Docker images tagging

Starting from version 2.0.0, a new scheme is used for tagging of Docker images:
* Releases are tagged with the version number, e.g., `stellio/stellio-search-service:2.0.0`
* `latest` tag is no longer used for releases as it can be dangerous (for instance, triggering an unwanted major
  upgrade)
* Developement versions (automatically produced when a commit is pushed on the `develop` branch) are tagged with a 
  tag containing the `-dev` suffix, e.g., `stellio/stellio-search-service:2.1.0-dev`
* On each commit on the `develop` branch, an image with the `latest-dev` tag is produced, e.g., `stellio/stellio-search-service:latest-dev`

The version number is obtained during the build process by using the `version` information in the `build.gradle.kts` file.

## Development

### Developing on a service

Requirements:
* Java 17 (we recommend using [sdkman!](https://sdkman.io/) to install and manage versions of the JDK)

To develop on a specific service, you can use the provided `docker-compose.yml` file inside each service's directory, for instance:

```shell script
cd search-service
docker-compose up -d && docker-compose logs -f
```

Then, from the root directory, launch the service:

```shell script
./gradlew search-service:bootRun
```

### Running the tests

Each service has a suite of unit and integration tests. You can run them without manually launching any external component, thanks
to Spring Boot embedded test support and to the great [TestContainers](https://www.testcontainers.org/) library.

For instance, you can launch the test suite for entity service with the following command:
 
```shell script
./gradlew search-service:test
```

### Building the project

To build all the services, you can just launch:

```shell script
./gradlew build
```

It will compile the source code, check the code quality (thanks to [detekt](https://detekt.dev/)) and run the test
suite for all the services.

For each service, a self executable jar is produced in the `build/libs` directory of the service.

If you want to build only one of the services, you can launch:

```shell script
./gradlew search-service:build
```

### Code quality

Code formatting and standard code quality checks are performed by [Detekt](https://detekt.github.io/detekt/index.html).

Detekt checks are automatically performed as part of the build and fail the build if any error is encountered.

* You may consider using a plugin like [Save Actions](https://plugins.jetbrains.com/plugin/7642-save-actions) 
that applies changed code refactoring and optimized imports on a save.

* You can enable Detekt support with the [Detekt plugin](https://github.com/detekt/detekt-intellij-plugin).

### Working locally with Docker images

To work locally with a Docker image of a service without publishing it to Docker Hub, you can follow the below instructions:

* Build a tar image:

```shell script
./gradlew search-service:jibBuildTar
```

* Load the tar image into Docker:

```shell script
docker load --input search-service/build/jib-image.tar
```

* Run the image:

```shell script
docker run stellio/stellio-search-service:latest
```

## Releasing a new version

* Merge develop into master 

```
git checkout master
git merge develop
```

* Update version number in `build.gradle.kts` (`allprojects.version` near the bottom of the file)
* Commit the modification using the following template message

```
git commit -am "chore: upgrade version to x.y.z"
```

* Push the modifications

```
git push origin master
```

The CI will then create and publish Docker images tagged with the published version number in https://hub.docker.com/u/stellio.

* On GitHub, check and publish the release notes in https://github.com/stellio-hub/stellio-context-broker/releases

## Usage

To start using Stellio, you can follow the [API quick start](https://github.com/stellio-hub/stellio-docs/blob/master/docs/quick_start_guide.md).

## Minimal hardware requirements needed to run Stellio

The recommended system requirements may vary depending on factors such as the scale of deployment, usage patterns, and specific use cases. That said, here are the general guidelines for the minimum computer requirements:

* Processor: Dual-core processor or higher
* RAM: 4GB or higher (1.8GB is needed to just run it)
* Storage: At least 4GB of free disk space (3.8GB is needed to just run it)
* Operating System: Linux (recommended), macOS (also recommended), or Windows

Please note that these requirements may vary based on factors such as the size of your dataset, the number of concurrent users, and the overall complexity of your use case.

## Further resources

For more detailed explanations on NGSI-LD or FIWARE:

-  [NGSI-LD v1.6.1 Specification](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.06.01_60/gs_CIM009v010601p.pdf)
-  [NGSI-LD Primer](https://www.etsi.org/deliver/etsi_gr/CIM/001_099/008/01.01.01_60/gr_CIM008v010101p.pdf)
-  [NGSI-LD Tutorials](https://ngsi-ld-tutorials.readthedocs.io/en/latest/)
-  [FIWARE Training](https://fiware-academy.readthedocs.io/en/latest/integrated-courses/fiware-training/index.html)

## License

Stellio is licensed under [APL-2.0](https://spdx.org/licenses/Apache-2.0.html).

It mainly makes use of the following libraries and frameworks (dependencies of dependencies have been omitted):

| Library / Framework | 	Licence     |
|---------------------|--------------|
| Spring              | APL v2       |
| JSON-LD Java        | BSD-3 Clause |
| Reactor             | APL v2       |
| Jackson             | 	APL v2      |
| JUnit               | EPL v2       |
| Mockk               | 	APL v2      |
| JsonPath            | 	APL v2      |
| WireMock            | APL v2       |
| Testcontainers      | 	MIT         |

© 2020 - 2023 EGM
