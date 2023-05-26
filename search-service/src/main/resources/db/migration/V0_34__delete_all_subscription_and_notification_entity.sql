delete from entity_payload
where types && array['https://uri.etsi.org/ngsi-ld/Subscription']
or types && array['https://uri.etsi.org/ngsi-ld/Notification'];
