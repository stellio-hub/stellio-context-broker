-- temporal entity attributes and access rights referencing non existent entities in entity_payload table

delete from temporal_entity_attribute
where entity_id not in (select entity_id from entity_payload);

delete from entity_access_rights
where entity_id not in (select entity_id from entity_payload);

-- attributes whose expanded version has changed

delete from temporal_entity_attribute tea1
where created_at is null
and exists(
    select 1
    from temporal_entity_attribute tea2
    where tea1.entity_id = tea2.entity_id
      and tea2.created_at is not null
      and substring(tea2.attribute_name from '[\w]+$') = substring(tea1.attribute_name from '[\w]+$')
);

-- entities related to authorization that no longer are regular entities

delete from temporal_entity_attribute
where entity_id in (
    select entity_id
    from entity_payload
    where types && ARRAY[
        'https://ontology.eglobalmark.com/authorization#Group',
        'https://ontology.eglobalmark.com/authorization#Client',
        'https://ontology.eglobalmark.com/authorization#User'
    ]
);

delete from entity_access_rights
where entity_id in (
    select entity_id
    from entity_payload
    where types && ARRAY[
        'https://ontology.eglobalmark.com/authorization#Group',
        'https://ontology.eglobalmark.com/authorization#Client',
        'https://ontology.eglobalmark.com/authorization#User'
        ]
);

delete from entity_payload
where types && ARRAY[
    'https://ontology.eglobalmark.com/authorization#Group',
    'https://ontology.eglobalmark.com/authorization#Client',
    'https://ontology.eglobalmark.com/authorization#User'
];

-- delete the few remaining entries

delete from temporal_entity_attribute
where created_at is null;
