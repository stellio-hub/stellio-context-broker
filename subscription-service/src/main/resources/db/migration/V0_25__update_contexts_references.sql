CREATE FUNCTION replace_in_contexts(contexts text[], old_value text, new_value text)
    RETURNS text[]
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$func$
SELECT ARRAY (
    SELECT regexp_replace(elem, old_value, new_value, 'g')
    FROM   unnest(contexts) elem
)
$func$;

UPDATE subscription
SET contexts = replace_in_contexts(contexts, 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld', 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld');

UPDATE subscription
SET contexts = replace_in_contexts(contexts, 'https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master', 'https://easy-global-market.github.io/ngsild-api-data-models');
