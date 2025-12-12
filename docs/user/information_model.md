# Information model

## JSON-LD
The information model is base on [json-ld](https://json-ld.org/). 
More precisely Stellio expect an `@context` describing the provided data. 
If you don't provide one Stellio will use the [ngsi-ld default context](https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld)

Using an `@context` enable semantic interoperability, you can find out more with this [tutorial](https://ngsi-ld-tutorials.readthedocs.io/en/latest/working-with-%40context.html)
## Model
### entity
The data is stored in the form of entities. An entity is represented by an `id` and a `type`

minimal entity :
```json
{
  "id":"urn:ngsi-ld:BeeHive:01",
  "type":"BeeHive"
}
```

```json
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "managedBy": {
     "type": "Relationship",
     "object": "urn:ngsi-ld:Beekeeper:01"
   },
   "location": {
      "type": "GeoProperty",
      "value": {
         "type": "Point",
         "coordinates": [
            24.30623,
            60.07966
         ]
      }
   },
   "temperature": {
      "type": "Property",
      "value": 22.2,
      "unitCode": "CEL",
      "observedAt": "2025-10-26T21:32:52.98601Z",
      "observedBy": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:Sensor:01"
      }
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```
### attribute
You can describe information belonging to the entity via attribute. 
An attribute can be a `Property` or a `Relationship`.
#### Property
A property is any information who possessed a value
There is X types of Property:
- Property
- JsonProperty
- GeoProperty

example: <!-- todo add all property types -->
```json
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "location": {
      "type": "GeoProperty",
      "value": {
         "type": "Point",
         "coordinates": [
            24.30623,
            60.07966
         ]
      }
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```
#### Relationship
A relationship represent a link to another entity in Stellio.

```json
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "managedBy": {
     "type": "Relationship",
     "object": "urn:ngsi-ld:Beekeeper:01"
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```

#### SubAttribute
Attributes can possess sub attributes, for example :

```json
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "temperature": {
      "type": "Property",
      "value": 22.2,
      "unitCode": "CEL",
      "observedAt": "2025-10-26T21:32:52.98601Z",
      "observedBy": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:Sensor:01"
      }
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```

#### Multi attribute
You can define different values for the same attribute with the help of datasetId:
<!-- todo -->
```json
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "managedBy": {
     "type": "Relationship",
     "object": "urn:ngsi-ld:Beekeeper:01"
   },
   "temperature": {
      "type": "Property",
      "value": 22.2
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```

## Going further
To go further you can follow the [api walkthrough](API_walkthrough.md)