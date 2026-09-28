# Service registration and execution (beta)

> **Beta — Stellio extension:** Service registration and execution are not yet part of the NGSI-LD specification.
> **Everything is subject to change.** This include this documentation as well as the Service execution and registration api.

## Manage services

### Introduction

Stellio exposes endpoints to register HTTP services and execute them for a target entity.
Registering a service does not invoke it.

The examples use a Stellio instance with authentication disabled and an HTTP service reachable from
Stellio's search service. For a secured broker, see
[Authentication and authorization](authentication_and_authorization.md).

Requests use `Content-Type: application/json` and responses use `Accept: application/json`.
The examples use compact types with the broker's default JSON-LD context. Use the same context for
registration, discovery, and execution. To use a custom context, provide a `Link` header as shown in
the [API walkthrough](API_walkthrough.md).

#### Terminology

- `service registration`: describes an HTTP service, the entities it applies to, and its input and output schemas.
- `service execution`: invokes a registered service and stores its status and result in Stellio.

### Endpoints for service registration management

Service registrations are represented by a `ServiceRegistration` data type.

#### ServiceRegistration data type

The following properties are used:

- `id`: a unique URI identifying the registration. If omitted, Stellio generates a URN.
- `type`: should always be "ServiceRegistration" if provided.
- `endpoint`: required absolute URI of the HTTP service to invoke.
- `endpointMethod`: HTTP method used to invoke the service. Defaults to `POST`.
- `entities`: required non-empty list of entity selectors. Each selector requires a `type` and can
  restrict matching with `id` or `idPattern`.
- `serviceInformation`: describes the service:
    - `name`: required, non-empty service name.
    - `title`: optional title of the service.
    - `description`: optional description of the service.
    - `mode`: `synchronous` or `asynchronous`. Defaults to synchronous behavior.
    - `input`: optional [JSON Schema](https://json-schema.org/) describing the request data. 
    It is used to validate the input data when creating a service execution.
    - `output`: optional [JSON Schema](https://json-schema.org/) describing the response data.

Today `input` and `output` supports most of the Json-schema specification (Except that external references are disabled.)

> **Potential changes to the Registration data type** :
> - `endpointMethod` is not yet part of the discussed implementation and may be removed in the future.
    (If you have use cases for it, please [create an issue](https://github.com/stellio-hub/stellio-context-broker/issues) 
     to let us know.)
> - What part of json-schema will be supported in the ngsi-ld specification is not decided yet.
    So the current implementation may allow schemas that will be rejected in future releases.

#### Service registration provision

##### Create a service registration

- POST /ngsi-ld/v1/serviceRegistrations

The following example contains only the required fields:

```json
{
  "endpoint": "http://lamp-service:8080/brightness",
  "entities": [
    {
      "type": "Lamp"
    }
  ],
  "serviceInformation": {
    "name": "setBrightness"
  }
}
```

Replace `endpoint` with your service's URL. This example uses the default `POST` method and
synchronous execution, without input or output schemas.

<details>
<summary>More complete registration with input and output schemas</summary>

This example adds an explicit ID, a list of target entities, service metadata, and JSON Schemas
for input and output.

```json
{
  "id": "urn:ngsi-ld:ServiceRegistration:setBrightness",
  "type": "ServiceRegistration",
  "endpoint": "http://lamp-service:8080/brightness",
  "endpointMethod": "POST",
  "entities": [
    {
      "id": ["urn:ngsi-ld:Lamp:1", "urn:ngsi-ld:Lamp:2"],
      "type": "Lamp"
    }
  ],
  "serviceInformation": {
    "name": "setBrightness",
    "title": "Set lamp brightness",
    "description": "Set brightness from 0 (off) to 255.",
    "mode": "synchronous",
    "input": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "properties": {
        "lampId": { "type": "string" },
        "brightness": { "type": "integer", "minimum": 0, "maximum": 255 }
      },
      "required": ["lampId", "brightness"],
      "additionalProperties": false
    },
    "output": {
      "type": "object",
      "properties": {
        "accepted": { "type": "boolean" }
      },
      "required": ["accepted"]
    }
  }
}
```

</details>

##### Update a service registration

- PATCH /ngsi-ld/v1/serviceRegistrations/{id}

```json
{
  "endpoint": "http://lamp-service:8080/v2/brightness"
}
```

Note:

- Supplied top-level fields entirely replace their previous values.
- When updating `serviceInformation`, provide the complete object, including `name` and any mode or
  schemas to retain. Nested fields are not merged individually.

##### Delete a service registration

- DELETE /ngsi-ld/v1/serviceRegistrations/{id}

#### Service registration consumption

##### Retrieve a service registration

- GET /ngsi-ld/v1/serviceRegistrations/{id}

Add `options=sysAttrs` to include `createdAt` and `modifiedAt` in the response.

##### Query service registrations

- GET /ngsi-ld/v1/serviceRegistrations

This endpoint discovers services applicable to target entities. The current implementation requires
both of the following query parameters:

- `id`: a target entity ID, or a comma-separated list of target entity IDs.
- `type`: the target entity type selection.

For example:

```http
GET /ngsi-ld/v1/serviceRegistrations?id=urn:ngsi-ld:Lamp:1&type=Lamp
```

This request finds registrations whose `entities` selectors match the target ID and type together.
The response is an array of matching registrations. These parameters do not filter by registration
ID or by the `ServiceRegistration` type; use the retrieval endpoint to get a registration by its ID.

Other parameters:

- `count=true`: includes the total count in the `NGSILD-Results-Count` response header.
- `options=sysAttrs`: includes `createdAt` and `modifiedAt`.

This endpoint supports pagination with `limit` and `offset`.

### Endpoints for service execution management

Service executions are represented by a `ServiceExecution` data type.

#### ServiceExecution data type

The following properties are used:

- `id`: a unique URI identifying the execution. If omitted, Stellio generates a URN.
- `type`: should always be "ServiceExecution" if provided.
- `serviceId`: required ID of the service registration to invoke.
- `entityId`: required ID of the target entity.
- `entityType`: required type of the target entity.
- `input`: required, non-null data sent to the service.
- `executionStatus`: `pending`, `executing`, `success`, `failure`, or `cancelled`.
- `output`: response data from the service.
- `progress`: a number between `0` and `1`, only used in asynchronous mode.
- `responseStatusCode`: HTTP status code returned by the service.

> **Potential changes to the ServiceExecution data type** :
> - `progress` is not yet discussed and might change when integrating the spec
> - Error handling behaviors is not completely defined and will probably be subject to change,
> - this includes the `responseStatusCode` and the way error message is relaid inside the service execution.

##### Execution modes

The registration's `serviceInformation.mode` determines how Stellio handles the endpoint response:

- `synchronous`: a 2xx response sets the status to `success`. Other HTTP statuses set it to `failure`.
  The endpoint response becomes `output`.
- `asynchronous`: a 2xx acknowledgement sets the status to `executing`. Other HTTP statuses set it to
  `failure`. The original response body becomes the initial `output`. 
   The request to the service-executor contains the `Service-Execution` header.
   The service executor should use this id to update the service progress via the PATCH endpoint.

#### Service execution provision

##### Create a service execution

- POST /ngsi-ld/v1/services

```json
{
  "id": "urn:ngsi-ld:ServiceExecution:brightness-001",
  "type": "ServiceExecution",
  "serviceId": "urn:ngsi-ld:ServiceRegistration:setBrightness",
  "entityId": "urn:ngsi-ld:Lamp:1",
  "entityType": "Lamp",
  "input": {
    "lampId": "urn:ngsi-ld:Lamp:1",
    "brightness": 125
  }
}
```

Stellio calls the registered endpoint, with the `input` value as the request body.

For example the previous service creation call the service executor with this body: 
```json
{
    "lampId": "urn:ngsi-ld:Lamp:1",
    "brightness": 125
}
```

A successful execution resource creation returns the current state of the execution.
For example if the synchronous service returned HTTP 200 with `{"accepted":true}`, the response
would contain the following result field:

```json
{
  "executionStatus": "success",
  "output": { "accepted": true },
  "responseStatusCode": 200
}
```

Note:

- Result fields (`executionStatus`, `progress`, `output`, and `responseStatusCode`) must not be supplied on creation.
- A 201 response confirms creation of the execution resource. Check `executionStatus` and
  `responseStatusCode` to determine whether the service succeeded.
- An endpoint error sets the status to `failure` and stores the endpoint's response.
- A connection error sets the status to `failure`, with a `responseStatusCode` of `504` and a
  problem-details object in `output`.
- JSON responses are stored as JSON, non-JSON responses are stored as text, and an empty response has no output value.

##### Update a service execution

- PATCH /ngsi-ld/v1/services/{id}

An executor can use this endpoint to report the completion of an asynchronous execution:

```json
{
  "executionStatus": "success",
  "progress": 1.0,
  "output": { "accepted": true },
  "responseStatusCode": 200
}
```

Note:
- Only `executionStatus`, `progress`, `output`, and `responseStatusCode` can be updated.

##### Delete a service execution

> **Warning** The cancellation behavior is not well discussed yet so this endpoint will change.

- DELETE /ngsi-ld/v1/services/{id}?options=remove

Removing an execution record does not stop the external service.

Note:

- Always specify `options=remove` to delete an execution record.
- Other possible options are `options=remove,cancel` and `options=cancel`
  but they will result in a 501 not implemented error

#### Service execution consumption

##### Retrieve a service execution

- GET /ngsi-ld/v1/services/{id}

Returns the latest stored status and result. Add `options=sysAttrs` to include `createdAt` and `modifiedAt`.

##### Query service executions

- GET /ngsi-ld/v1/services

You can filter executions with the following query parameters:

- `id`: comma separated list of execution ID.
- `serviceId`: comma separated list of service registration ID.
- `entityId`: comma separated list of entity ID.
- `executionStatus`: comma separated list of execution statuses, for example `success,failure`.

With no filters, the endpoint lists all executions using pagination.

Other parameters:

- `count=true`: includes the total count in the `NGSILD-Results-Count` response header.
- `options=sysAttrs`: includes `createdAt` and `modifiedAt`.

This endpoint supports pagination with `limit` and `offset`.

### Current beta limitations

- Registration discovery uses entity IDs and types. Registration fields `q`, `geoQ`, and `scopeQ` can
  be stored but are not evaluated.
- Execution selects a registration by `serviceId`; it does not check that the target entity matches
  that registration's discovery selectors.
- Output validation against JSON Schema, name-based service resolution, and cancellation are not implemented.
