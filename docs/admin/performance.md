# Performance

## Principles

Performance in Stellio is mainly related to the performance of the underlying database (PostgreSQL / Timescale). However, the database settings to achieve good performance when writing data (as few indexes as possible) are often different to the settings needed to achieve good performance when reading data (specific indexes tuned to data retrieval).

Thus, by default, the Stellio databases are configured to provide a good balance between writing and reading. If you have more specific scenarios in one or another area, you may have to adapt these settings. You may also have to fine tune the settings of PostgreSQL / Timescale (for instance, sizes of chunks to store the history of attributes can be a very important one).

## Load test campaign

An NGSI-LD load test suite is available in [GitHub](https://github.com/stellio-hub/ngsild-load-tests). In the README, you will find instructions on how to install and configure it. There is a description of all the scenarions that are currently available. They are currently focused on the most typical endpoints used in everyday life of a project but it is planned to add new ones soon (for instance, the Merge Entity endpoint or the subscription endpoints).

The objective is to be as close as possible to data used in real life projects. Thus, the [template entity](https://github.com/stellio-hub/ngsild-load-tests/blob/master/src/data/template_entity.json) that is used in the different scenarios is composed of 6 properties (some being multi-instance ones), 1 relationship and 1 geolocation.

## Current state of Stellio

The results presented here have been obtained in September 2024 using the following setup:

* Stellio: 8 vCPU / 16 GB RAM / 160 GB disk SSD NVMe / Debian 12 (VPS Elite from OVH)
* Load tests: 4 vCPU / 8 GB RAM / 160 GB disk SSD NVMe / Debian 12 (VPS Comfort from OVH)

Each scenario has been run 3 times, the numbers presented below are an average of the 3 runs for each scenario.

### Create entity

| VUs    | Number of entities created     | Requests / second    | Response time (95th percentile) | 
|--------|--------------------------------|----------------------|---------------------------------|
| 10     | 10000                          | 131                  | 137 ms                          |
| 10     | 100000                         | 221                  | 70 ms                           |
| 50     | 100000                         | 241                  | 297 ms                          |

### Partial attribute update

| VUs    | Initial setup     | Number of updates | Requests / second | Response time (95th percentile) |
|--------|-------------------|-------------------|-------------------|---------------------------------|
| 10     | 1000 entities     | 10000             | 120               | 79 ms                           |
| 10     | 1000 entities     | 100000            | 375               | 36 ms                           |
| 50     | 1000 entities     | 100000            | 417               | 157 ms                          |

### Update attributes

| VUs    | Initial setup     | Number of updates | Requests / second | Response time (95th percentile) |
|--------|-------------------|-------------------|-------------------|---------------------------------|
| 10     | 1000 entities     | 10000             | 88                | 137 ms                           |
| 10     | 1000 entities     | 100000            | 179               | 80 ms                           |
| 50     | 1000 entities     | 100000            | 197               | 310 ms                          |

### Query entities by type and values of properties

The query looks like `/ngsi-ld/v1/entities?type={randomType}&q=aProperty>{randomValue};anotherProperty=={aRandomExistingValue}`

| VUs    | Initial setup                          | Number of queries | Requests / second | Response time (95th percentile) |
|--------|----------------------------------------|-------------------|-------------------|---------------------------------|
| 10     | 1000 entities<br>5 different types     | 10000             | 33                | 269 ms                          |
| 10     | 10000 entities<br>10 different types   | 100000            | 30                | 343 ms                          |
| 10     | 100000 entities<br>10 different types  | 10000             | 32                | 1440 ms                         |

### Query entities by type and object of a relationship

The query looks like `/ngsi-ld/v1/entities?type={randomType}&q=aProperty>{randomValue};aRelationship=={aRandomExistingObject}`

| VUs    | Initial setup                         | Number of queries | Requests / second | Response time (95th percentile) |
|--------|---------------------------------------|-------------------|-------------------|---------------------------------|
| 10     | 1000 entities<br>5 different types    | 10000             | 36                | 169 ms                          |
| 10     | 10000 entities<br>10 different types  | 100000            | 30                | 298 ms                          |
| 10     | 100000 entities<br>10 different types | 10000             | 32                | 547 ms                          |

### Retrieve temporal evolution of an entity (aggregated values)

The query looks like `/ngsi-ld/v1/temporal/entities/{entityId}?timerel=after&timeAt=2023-01-01T00:00:00Z&aggrMethods={randomAggregateMethod}&aggrPeriodDuration=PT1S&options=aggregatedValues`

| VUs    | Initial setup                           | Number of queries | Requests / second | Response time (95th percentile) |
|--------|-----------------------------------------|-------------------|-------------------|---------------------------------|
| 10     | 10 entities<br>1000 temporal instances  | 1000              | 137               | 88 ms                           |
| 10     | 10 entities<br>10000 temporal instances | 1000              | 86                | 137 ms                          |
| 10     | 10 entities<br>100000 temporal instances| 1000              | 42                | 321 ms                          |
