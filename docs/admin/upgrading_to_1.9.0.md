# Upgrading to 1.9.0

This note describes the necessary steps to upgrade to Stellio 1.9.0

## New endpoints for rights management

Stellio 1.9.0 brings new endpoints that help in managing rights inside the context broker.

### Get authorized entites for the currently authenticated user 

The first endpoint allows an user to get the rights it has on entities.

It is available under `/ngsi-ld/v1/entityAccessControl/entities` and can be called with a `GET` request.

The following request parameters are supported: 

* `q`: restrict returned entities to the ones with a specific right. Only `rCanRead` and `rCanWrite` and `rCanAdmin` are accepted. A list is accepted (e.g, `q=rCanRead;rCanWrite`). This request parameter has no effect when user has the _stellio-admin_ role
* `type`: restrict returned entities to a given entity type
* `options`: use `sysAttrs` value to get the system attributes

There are several possible answers:

* If user is not admin of the entity but it has a right on it (`rCanRead` or `rCanWrite`), the response body will be under this form:  

```JSON
[
    { 
        “id”: "urn:ngsi-ld:Entity:01",
        “type”: "Entity",
        “datasetId”: "urn:ngsi-ld:Dataset:Entity:01",
        “right”: { 
            “type”: “Property”, 
            “value”: “rCanWrite” 
        }, 
        “specificAccessPolicy”: { 
            “type”: “Property”, 
            “value”: “AUTH_READ” 
        }, 
        @context: [ "https://my.context/context.jsonld" ] 
    },
    {
        ...
    }
]
```

* If user is admin of the entity, the response body will be under this form: 

```json
[
    { 
        “id”: "urn:ngsi-ld:Entity:01",
        “type”: "Entity",
        “datasetId”: "urn:ngsi-ld:Dataset:Entity:01",
        “right”: { 
            “type”: “Property”, 
            “value”: “rCanAdmin” 
        }, 
        “specificAccessPolicy”: { 
            “type”: “Property”, 
            “value”: “AUTH_READ” 
        }, 
        “rCanRead”: [  
            {  
                “type”: “Relationship”,  
                “object”: “urn:ngsi-ld:User:01”     
            }, 
            {  
                “type”: “Relationship”,  
                “object”: “urn:ngsi-ld:User:02”
            } 
        ], 
        “rCanWrite”: [ 
            … 
        ], 
        “rCanAdmin”: [ 
            … 
        ], 
        @context: [ "https://my.context/context.jsonld" ] 
    },
    {
        ...
    }
]
```

The body also contains the other users who have a right on the entity.

* If authentication is not enabled, a 204 (No content) response is returned. 


### Get groups the currently authenticated user belongs to 

This endpoint allows an user to get the groups it belongs to.

It is available under `/ngsi-ld/v1/entityAccessControl/groups` and can be called with a `GET` request.

There are several possible answers:

* If user is not _stellio-admin_: 

```json
[
    { 
        “id”: "urn:ngsi-ld:Group:01",
        “type”: "Group",
        “name”: { 
            “type”: “Property”, 
            “value”: "EGM" 
        }, 
        @context: [ "https://my.context/context.jsonld" ] 
    }
]
```

* If user is _stellio-admin_, all groups are returned:

```json
[
    { 
        “id”: "urn:ngsi-ld:Group:01",
        “type”: "Group",
        “name”: { 
            “type”: “Property”, 
            “value”: “EGM” 
            }, 
        “isMemberOf”: { 
            “type”: “Property”, 
            “value”: true
        }, 
        @context: [ "https://my.context/context.jsonld" ] 
    }
]
```

The body also contains membership information. 

* If authentication is not enabled, a 204 (No content) response is returned. 
