delete from attribute_instance
where temporal_entity_attribute in
    ( select id from temporal_entity_attribute
    where entity_id in
        ( select entity_id from entity_payload
        where types && array['https://uri.etsi.org/ngsi-ld/Subscription'] or types && array['https://uri.etsi.org/ngsi-ld/notification']
        )
    );

delete from attribute_instance_audit
where temporal_entity_attribute in
    ( select id from temporal_entity_attribute
    where entity_id in
        ( select entity_id from entity_payload
        where types && array['https://uri.etsi.org/ngsi-ld/Subscription'] or types && array['https://uri.etsi.org/ngsi-ld/notification']
        )
    );

delete from temporal_entity_attribute
where entity_id in
    ( select entity_id from entity_payload
    where types && array['https://uri.etsi.org/ngsi-ld/Subscription'] or types && array['https://uri.etsi.org/ngsi-ld/notification']
    );

delete from entity_access_rights
where entity_id in
    ( select entity_id from entity_payload
    where types && array['https://uri.etsi.org/ngsi-ld/Subscription'] or types && array['https://uri.etsi.org/ngsi-ld/notification']
    );

delete from entity_payload
where types && array['https://uri.etsi.org/ngsi-ld/Subscription']
or types && array['https://uri.etsi.org/ngsi-ld/Notification'];
