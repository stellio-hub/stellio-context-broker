# Upgrading to 1.6.0

This note describes the necessary steps to upgrade to Stellio 1.6.0

## Synchronization of specific access policies

In order to propagate specific access policies defined in entity service, an action has to be launched manually by an account having the `stellio-admin` role:

```
# Get an acces token first
export TOKEN=$(http --form POST https://{keycloak_domain_name}/auth/realms/{realm_name}/protocol/openid-connect/token client_id={client_id} client_secret={client_secret} grant_type=client_credentials | jq -r .access_token)

# Call the synchronization action
http POST https://{context_broker_domain_name}/ngsi-ld/v1/entityAccessControl/syncSap Authorization:"Bearer $TOKEN"
```

Check the logs of the entity and search services to be sure the synchronization has worked as expected. If not, feel free [to raise an issue](https://github.com/stellio-hub/stellio-context-broker/issues/new/choose) in the Stellio repository.
