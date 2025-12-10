# Upgrading to 2.11.0

This note describes the necessary steps to upgrade to Stellio 2.11.0

## Relaxing of tenants names

Starting from version 2.11.0, Stellio has applied the relaxing of tenants names that has been introduced in version 1.7.1 of the NGSI-LD specification. The name of a tenant can now be a string (before it had to be an URI). As a consequence, the variable defining the name of a tenant has been updated to reflect this change (basically, using a `name` prefix instead of a `uri` one).

In Docker based deployments, the name of the default tenant is now defined by the `APPLICATION_TENANTS_0_NAME` environment variable (it was previously defined by the `APPLICATION_TENANTS_0_URI` environment variable).

In the same way, apply the same kind of renaming for the other tenants declared in your Stellio instances.

## Strict checking of JSON-LD contexts

As part of the release of the 2.11.0 version, Stellio switched the library used to do JSON-LD processing from [JSONLD-Java](https://github.com/jsonld-java/jsonld-java) to [Titanium](https://github.com/filip26/titanium-json-ld).

The Titanium library does a strict checking of the JSON-LD specification and, in particular, it rejects JSON-LD contexts whose Content-Type is not `application/json` or a media type with a `+json` suffix (as described in [https://www.w3.org/TR/json-ld11-api/#loaddocumentcallback](https://www.w3.org/TR/json-ld11-api/#loaddocumentcallback)).

Especially, if you are using JSON-LD contexts from the Smart Data Models initiative, ensure to use the version that is correctly served. For instance, instead of using [https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld](https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld), use the version hosted on GitHub pages: [https://smart-data-models.github.io/dataModel.Environment/context.jsonld](https://smart-data-models.github.io/dataModel.Environment/context.jsonld).
