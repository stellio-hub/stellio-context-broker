alter table geometry_query add column pgis_geometry geometry;
update geometry_query
    set pgis_geometry = ST_GeomFromGeoJSON('{"type":"' || geometry || '","coordinates":' || coordinates || '}');
