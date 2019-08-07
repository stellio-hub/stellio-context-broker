# Sample queries

* Create a new entity

```
http POST http://localhost:8080/ngsi-ld/v1/entities Content-Type:application/json < src/test/resources/data/observation_sensor.jsonld
```

* Get an entity by type

```
http GET http://localhost:8080/ngsi-ld/v1/entities?type=BeeHive Content-Type:application/json
```

