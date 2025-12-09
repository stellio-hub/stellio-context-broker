# Stellio context broker

[![FIWARE Core Context Management](https://nexus.lab.fiware.org/repository/raw/public/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
[![License: Apache-2.0](https://img.shields.io/github/license/stellio-hub/stellio-context-broker.svg)](https://spdx.org/licenses/Apache-2.0.html)
![Status](https://nexus.lab.fiware.org/static/badges/statuses/full.svg)
[![NGSI-LD badge](https://img.shields.io/badge/NGSI-LD-red.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.06.01_60/gs_CIM009v010601p.pdf)
[![SOF support badge](https://nexus.lab.fiware.org/repository/raw/public/badges/stackoverflow/fiware.svg)](http://stackoverflow.com/questions/tagged/fiware)
<br>
[![Quay badge](https://img.shields.io/badge/quay.io-fiware%2Fstellio--*-grey?logo=red%20hat&labelColor=EE0000)](https://quay.io/search?q=stellio-)
[![Docker badge](https://img.shields.io/badge/docker-stellio%2Fstellio--*-blue?logo=docker)](https://hub.docker.com/u/stellio)
<br>
[![Documentation badge](https://readthedocs.org/projects/stellio/badge/?version=latest)](http://stellio.readthedocs.io/en/latest/?badge=latest)
![Build](https://github.com/stellio-hub/stellio-context-broker/workflows/Build/badge.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=stellio-hub_stellio-context-broker&metric=alert_status)](https://sonarcloud.io/dashboard?id=stellio-hub_stellio-context-broker)
[![CodeQL](https://github.com/stellio-hub/stellio-context-broker/actions/workflows/codeql.yml/badge.svg)](https://github.com/stellio-hub/stellio-context-broker/actions/workflows/codeql.yml)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4527/badge)](https://bestpractices.coreinfrastructure.org/projects/4527)

Stellio is an NGSI-LD compliant context broker developed by EGM. NGSI-LD is an Open API and Datamodel specification for 
context management [published by ETSI](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.08.01_60/gs_CIM009v010801p.pdf).

Stellio is a [FIWARE](https://www.fiware.org/) Generic Enabler. Therefore, it can be integrated as part of any platform “Powered by FIWARE”. 
FIWARE is a curated framework of open source platform components which can be assembled together with other third-party 
platform components to accelerate the development of Smart Solutions. For more information check the FIWARE Catalogue entry for
[Core Context](https://github.com/Fiware/catalogue/tree/master/core). The roadmap of this FIWARE GE is described [here](https://github.com/stellio-hub/stellio-context-broker/projects/1).

You can find more info at the [FIWARE developers](https://developers.fiware.org/) website and the [FIWARE](https://fiware.org/) website.
The complete list of FIWARE GEs and Incubated FIWARE GEs can be found in the [FIWARE Catalogue](https://catalogue.fiware.org/).

**NGSI-LD Context Broker Feature Comparison**

The NGSI-LD Specification is regularly updated and published by ETSI. The latest specification is [version 1.9.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_CIM009v010901p.pdf) which was published in July 2025.

-  An Excel file detailing the current compatibility of the development version of the Stellio Context Broker against the features of the 1.8.1 specification can be downloaded [here](https://docs.google.com/spreadsheets/d/e/2PACX-1vRxOjsDf3lqhwuypJ---pZN2OlqFRl0jyoTV0ewQ1WFnpe7xQary3uxRjunbgJkwQ/pub?output=xlsx)

| :books: [Documentation](https://stellio.rtfd.io/) | :whale: [Docker Hub](https://hub.docker.com/u/stellio) | :dart: [Roadmap](./docs/roadmap.md) |
|---------------------------------------------------|--------------------------------------------------------|-------------------------------------|

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
docker compose up -d && docker compose logs -f
```

It will start all the services composing the Stellio context broker platform and expose them on the following ports:
* API Gateway: 8080
* Search service: 8083
* Subscription service: 8084

Please note that the environment and scripts are validated on Ubuntu and macOS. Some errors may occur on other platforms.

We also provide a configuration to deploy Stellio in a k8s cluster. For more information, please look at the [stellio-k8s project](https://github.com/stellio-hub/stellio-k8s)

## Docker images tagging

Starting from version 2.0.0, the following scheme is used for tagging of Docker images:
* Releases are tagged with the version number, e.g., `stellio/stellio-search-service:2.0.0`
* `latest` tag is no longer used for releases as it can be dangerous (for instance, triggering an unwanted major upgrade)
* On each commit on the `develop` branch, an image with the `latest-dev` tag is produced, e.g., `stellio/stellio-search-service:latest-dev`

The version number is obtained during the build process by using the `version` information in the `build.gradle.kts` file.

## Usage

You can follow the [API walkthrough](https://github.com/stellio-hub/stellio-docs/blob/master/docs/API_walkthrough.md) to have a tutorial on the NGSI-LD API implemented by Stellio.

## Minimal hardware requirements

The recommended system requirements may vary depending on factors such as the scale of deployment, usage patterns, and specific use cases. That said, here are the general guidelines for the minimum computer requirements:

* Processor: Dual-core processor or higher
* RAM: 4GB or higher (1.8GB is needed to just run it)
* Storage: At least 4GB of free disk space (3.8GB is needed to just run it)
* Operating System: Linux (recommended), macOS (also recommended), or Windows

Please note that these requirements may vary based on factors such as the size of your dataset, the number of concurrent users, and the overall complexity of your use case.

## Further resources

For more detailed explanations on NGSI-LD or FIWARE:

-  [NGSI-LD v1.9.1 Specification](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_CIM009v010901p.pdf)
-  [NGSI-LD Primer](https://www.etsi.org/deliver/etsi_gr/CIM/001_099/008/01.03.01_60/gr_CIM008v010301p.pdf)
-  [NGSI-LD Tutorials](https://ngsi-ld-tutorials.readthedocs.io/en/latest/)
-  [FIWARE Webinars](https://www.fiware.org/community/webinars/)

## License

Stellio is licensed under [APL-2.0](https://spdx.org/licenses/Apache-2.0.html).

It mainly makes use of the following libraries and frameworks (dependencies of dependencies have been omitted):

| Library / Framework | Licence |
|---------------------|---------|
| Spring              | APL v2  |
| Titanium JSON-LD    | APL v2  |
| Reactor             | APL v2  |
| Jackson             | APL v2  |
| JUnit               | EPL v2  |
| Mockk               | APL v2  |
| JsonPath            | APL v2  |
| WireMock            | APL v2  |
| Testcontainers      | MIT     |

© 2020 - 2025 EGM

## Contribution and development

Stellio is an open source project you are welcome to contribute to the project. 
- You can find guideline for development [here](docs/files/development_guide.md). 
- And good practice for contribution [here](CONTRIBUTING.md).