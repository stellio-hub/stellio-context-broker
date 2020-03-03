# Quick start

* Clone the project
* Launch the provided `docker-compose.yml` file
* After the first start, connect to the Timescale Docker container and init the database (cf `src/main/resources/db/migration/V1__init.sql`)

# Sample queries

* Search between two dates:

```shell script
http http://localhost:8083/ngsi-ld/v1/temporal/entities/urn:sosa:Sensor:10e2073a01080065\?time\=2019-10-16T07:31:39%2B05:00\&timerel\=between\&endTime\=2019-10-19T07:31:39%2B05:00
```

* Search after a date:

```shell script
http http://localhost:8083/ngsi-ld/v1/temporal/entities/urn:sosa:Sensor:10e2073a01080065\?time\=2019-10-17T07:31:39%2B05:00\&timerel\=after
```

* Search before a date:

```shell script
http http://localhost:8083/ngsi-ld/v1/temporal/entities/urn:sosa:Sensor:10e2073a01080065\?time\=2019-10-17T07:31:39%2B05:00\&timerel\=before
```

* Issue an invalid request

```shell script
http http://localhost:8083/ngsi-ld/v1/temporal/entities/urn:sosa:Sensor:10e2073a01080065\?time\=2019-10-17T07:31:39%2B05:00
```
