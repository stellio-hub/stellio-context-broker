update geometry_query
    set geoproperty = 'https://uri.etsi.org/ngsi-ld/location';

alter table geometry_query
alter column geoproperty SET NOT null

