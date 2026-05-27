# Query Language

Stellio implements the NGSI-LD Query Language as defined in [ETSI GS CIM 009 §4.9](https://cim.etsi.org/NGSI-LD/official/clause-4.html#4.9).
It is available through the `q` query parameter on:

- `GET /ngsi-ld/v1/entities`
- `GET /ngsi-ld/v1/temporal/entities`
- `POST /ngsi-ld/v1/entityOperations/query`
- Subscription `q` field

---

## Operators

| Operator | Meaning                           |
|----------|-----------------------------------|
| `;`      | AND — both sides must match       |
| `\|`     | OR — at least one side must match |
| `( )`    | Grouping                          |

Operator precedence: `;` (AND) binds more tightly than `|` (OR). Use parentheses to override.

```
# temperature above 20 AND humidity below 80
q=temperature>20;humidity<80

# temperature outside 10–30 OR device is inactive
q=temperature<10|temperature>30|(status=="inactive")
```

---

## Existence checks

```
# entity has a temperature attribute
q=temperature

# entity does NOT have a temperature attribute
q=!temperature
```

---

## Comparison operators

| Syntax           | Meaning                           |
|------------------|-----------------------------------|
| `attr==value`    | Equal                             |
| `attr!=value`    | Not equal                         |
| `attr>value`     | Greater than                      |
| `attr>=value`    | Greater than or equal             |
| `attr<value`     | Less than                         |
| `attr<=value`    | Less than or equal                |
| `attr~=pattern`  | Matches regular expression        |
| `attr!~=pattern` | Does not match regular expression |

---

## Value types

Values are typed automatically; quoting a value forces it to be treated as a string.

| Type     | Example                 | Notes                                                                           |
|----------|-------------------------|---------------------------------------------------------------------------------|
| Number   | `42`, `3.14`, `-5`      | Integer or decimal                                                              |
| Boolean  | `true`, `false`         | Unquoted only                                                                   |
| String   | `"active"`, `"open"`    | Must be double-quoted                                                           |
| DateTime | `2025-01-01T00:00:00Z`  | ISO 8601 with timezone                                                          |
| Date     | `2025-01-01`            | ISO 8601 date only                                                              |
| Time     | `12:00:00Z`             | ISO 8601 time only                                                              |
| URI      | `urn:ngsi-ld:Sensor:01` | Matched against Relationship objects, Property values, and VocabProperty values |

```
q=temperature==22.5
q=status=="active"
q=createdAt>2025-01-01T00:00:00Z
q=managedBy=="urn:ngsi-ld:Beekeeper:01"
```

---

## Range values

A range matches values inclusively between a lower and upper bound, separated by `..`.
NEQ (`!=`) matches values outside the range.

```
# temperature between 18 and 26
q=temperature==18..26

# humidity outside 30–70
q=humidity!=30..70

# entries from January 2025
q=observedAt==2025-01-01T00:00:00Z..2025-01-31T23:59:59Z
```

---

## List values

A comma-separated list matches any of the listed values.
NEQ (`!=`) requires the attribute to match none of them.

```
# status is either "active" or "standby"
q=status=="active","standby"

# device is neither sensor-01 nor sensor-02
q=device!="urn:ngsi-ld:Sensor:01","urn:ngsi-ld:Sensor:02"
```

---

## Composed attribute paths

A dot-separated path traverses sub-attributes. Each segment is a compact term resolved against the request context.

```
# sub-attribute 'precision' of 'temperature' has a value above 30
q=temperature.precision>90

# sub-attribute 'unitCode' of 'temperature' equals "CEL"
q=temperature.unitCode=="CEL"
```

---

## Language tag filtering

Append `[lang]` to a LanguageProperty attribute to filter by language tag.
Only single-value comparisons and regex operators are supported; range and list are not defined by the spec.

```
# French name equals "Ruche"
q=name[fr]=="Ruche"

# English description contains "hive" (regex)
q=description[en]~=".*hive.*"

# English description does not contain "inactive"
q=description[en]!~=".*inactive.*"
```

---

## JSON property key access

Add a `[key]` bracket suffix to access a specific key inside a JsonProperty value.
The `jsonKeys` query parameter must list the attribute names that are JsonProperty attributes so the broker can 
route the query through the JSON path instead of the standard property value path.

```
# 'metadata' is a JsonProperty; query its 'version' key
GET /ngsi-ld/v1/entities?q=metadata[version]==2&jsonKeys=metadata

# 'config' key 'threshold' is between 10 and 50
GET /ngsi-ld/v1/entities?q=config[threshold]==10..50&jsonKeys=config
```

The `jsonKeys` parameter accepts a comma-separated list when multiple JsonProperty attributes are queried:

```
GET /ngsi-ld/v1/entities?q=metadata[version]==2;config[mode]=="auto"&jsonKeys=metadata,config
```

---

## Expanding compact values to IRIs

When comparing against VocabProperty values, comparison values that are compact terms (not already URIs) can be
expanded to IRIs at query time using the `expandValues` parameter.

```
# 'category' is a VocabProperty; "BeeHive" is a compact term in the request context
GET /ngsi-ld/v1/entities?q=category=="BeeHive"&expandValues=category
# equivalent to: q=category=="https://ontology.example.org/BeeHive"
```
