alter table subscription
add column contexts text[];

update subscription
set contexts = array['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld'];

