# Information model

## JSON-LD
The information model is base on [json-ld](https://json-ld.org/). 
More precisely Stellio expect an `@context` describing the provided data. 
If you don't provide one Stellio will use the [ngsi-ld default context](https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld).

Using an `@context` enable semantic interoperability, you can find out more with this [tutorial](https://ngsi-ld-tutorials.readthedocs.io/en/latest/working-with-%40context.html).
## Model
### entity
The data is stored in the form of entities. An entity is represented by an `id` and a `type`.

minimal entity:
```json
{
  "id":"urn:ngsi-ld:BeeHive:01",
  "type":"BeeHive"
}
```

### attribute
You can describe information belonging to the entity via attributes. 
An attribute can be a `Property` or a `Relationship`.

#### Property
A property is any information who possessed a value.

There multiple types of Property:

- ``Property``: a string, number, date, time, datetime or a boolean
- ``JsonProperty``: a valid json
- ``GeoProperty``: a GeoJSON geometry (use `location` to represent the location of the entity)
- ``LanguageProperty``: a text in multiple languages

example for different attributes:
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

- a unitCode property (it should have to follow [the common codes for units of measurement of UNECE/CEFACT](https://www.unece.org/fileadmin/DAM/cefact/recommendations/rec20/rec20_Rev9e_2014.xls?__cf_chl_tk=MMGgtNzYV_GbFE.GlTg4dCrEy.s50PElEClw44dp440-1765788868-1.0.1.1-PYn_GV8dqKssSgMl0XPZkkbkpT5chpBBYLVk35YMSe4)).
- an observedAt representing a UTC timestamp (using the ISO 8601 Extended format) 

#### Relationship
A relationship represent a link to another entity in Stellio.

example of relationship:
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
Attributes can have sub attributes, for example:

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
You can define different values for the same attribute with the help of datasetIds:
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
To go further you can follow the [api walkthrough](API_walkthrough.md).