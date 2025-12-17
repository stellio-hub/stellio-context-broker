# Information model

## JSON-LD
The information model is based on [JSON-LD](https://json-ld.org/).

Stellio expects an `@context` describing the provided data.
If you do not provide one, Stellio uses the [NGSI-LD default context](https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld).

Using an `@context` enables semantic interoperability. You can find out more in this [tutorial](https://ngsi-ld-tutorials.readthedocs.io/en/latest/working-with-%40context.html).

## Model
### Entity
Data is stored as entities. An entity is identified by an `id` and a `type`.

Minimal entity:
```json
{
  "id":"urn:ngsi-ld:BeeHive:01",
  "type":"BeeHive"
}
```

### Attribute
You describe information about an entity with attributes.
An attribute is either a `Property` or a `Relationship`.

#### Property
A property is any piece of information that has a value.

There are several property types:

- `Property`: a string, number, date, time, datetime, or boolean
- `JsonProperty`: valid JSON data
- `GeoProperty`: a GeoJSON geometry (use `location` to represent the entity’s location)
- `LanguageProperty`: text in multiple languages

Example with different attributes:
```json
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "hornetDetected": {
      "type": "Property",
      "value": true
   },
   "temperature": {
      "type": "Property",
      "value": 22.2,
      "unitCode": "CEL",
      "observedAt": "2025-10-26T21:32:52.98601Z"
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
   "json": {
      "type": "JsonProperty",
      "value": {
         "include": "any json"  
      }
   },
   "name": {
      "type": "LanguageProperty",
      "languageMap": {"en": "Beehive", "fr": "ruche"}
   },
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```

Properties can include:

- a `unitCode` (it should follow the [UNECE/CEFACT Common Code list for units of measurement](https://www.unece.org/fileadmin/DAM/cefact/recommendations/rec20/rec20_Rev9e_2014.xls?__cf_chl_tk=MMGgtNzYV_GbFE.GlTg4dCrEy.s50PElEClw44dp440-1765788868-1.0.1.1-PYn_GV8dqKssSgMl0XPZkkbkpT5chpBBYLVk35YMSe4)).
- an `observedAt` timestamp in UTC (ISO 8601 extended format)

#### Relationship
A relationship represents a link to another entity in Stellio.

Example of a relationship:
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

#### Sub-attribute
Attributes can have sub-attributes. For example:

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

#### Multi-attribute
You can assign multiple values to the same attribute by using `datasetId`s:
```json
{
   "id": "urn:ngsi-ld:BeeHive:01",
   "type": "BeeHive",
   "managedBy": [
      {
         "type": "Relationship",
         "object": "urn:ngsi-ld:Beekeeper:01"
      },{
        "type": "Relationship",
        "object": "urn:ngsi-ld:Beekeeper:02",
        "datasetId": "urn:ngsi-ld:second"
      }
   ],
   "temperature": [
      {
         "type": "Property",
         "value": 22.2,
         "datasetId": "urn:ngsi-ld:Sensor:1"
      },
      {
         "type": "Property",
         "value": 21.7,
         "datasetId": "urn:ngsi-ld:Sensor:2"
      }
   ],
   "@context": [
      "https://easy-global-market.github.io/ngsild-api-data-models/apic/jsonld-contexts/apic-compound.jsonld"
   ]
}
```

## Going further
For a practical walkthrough, see the [API walkthrough](API_walkthrough.md).
