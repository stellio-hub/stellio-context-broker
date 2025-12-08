# Upgrading to 1.7.0

This note describes the necessary steps to upgrade to Stellio 1.7.0

## Add support for recurring notifications

Stellio now supports the `timeInterval` property for subscriptions (for more details on this feature, you can refer to the NGSI-LD specification, section 5.2.11).

In order to get matching NGSI-LD entities for such subscriptions, the subscription service calls the entity service and thus needs to know how to do so.

That's why five properties have been added in subscription service: 

```
# Client registration used to get entities from entity-service
spring.security.oauth2.client.registration.keycloak.authorization-grant-type=client_credentials
spring.security.oauth2.client.registration.keycloak.client-id=client-id
spring.security.oauth2.client.registration.keycloak.client-secret=client-secret
spring.security.oauth2.client.provider.keycloak.token-uri=https://my.sso/token
application.entity.service-url=http://localhost:8082
```

If using Stellio in docker compose mode with authentication enabled, here is how you can configure them: 

```yaml
  subscription-service:
    environment:
      - SPRING_SECURITY_OAUTH2_CLIENT_REGISTRATION_KEYCLOAK_AUTHORIZATION-GRANT-TYPE=client_credentials
      - SPRING_SECURITY_OAUTH2_CLIENT_REGISTRATION_KEYCLOAK_CLIENT-ID=client-id
      - SPRING_SECURITY_OAUTH2_CLIENT_REGISTRATION_KEYCLOAK_CLIENT-SECRET=client-secret
      - SPRING_SECURITY_OAUTH2_CLIENT_PROVIDER_KEYCLOAK_TOKEN-URI=https://my.sso/token
      - APPLICATION_ENTITY_SERVICE-URL=http://entity-service:8082
```

If using Stellio in docker compose mode without authentication, you can just configure the URL of entity service:

```yaml
  subscription-service:
    environment:
      - APPLICATION_ENTITY_SERVICE-URL=http://entity-service:8082
```
