package com.egm.stellio.shared.model.parameter

import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonUtils

data class GeoQuery(
    val georel: String,
    val geometry: GeometryType,
    val coordinates: String,
    val wktCoordinates: WKTCoordinates,
    var geoproperty: ExpandedTerm = NGSILD_LOCATION_PROPERTY
) {
    enum class GeometryType(val type: String) {
        POINT("Point"),
        MULTIPOINT("MultiPoint"),
        LINESTRING("LineString"),
        MULTILINESTRING("MultiLineString"),
        POLYGON("Polygon"),
        MULTIPOLYGON("MultiPolygon");

        companion object {
            fun isSupportedType(type: String): Boolean =
                entries.any { it.type == type }

            fun forType(type: String): GeometryType? =
                entries.find { it.type == type }
        }
    }

    companion object {


        fun buildSqlFilter(geoQuery: GeoQuery, target: ExpandedEntity? = null): String {
            val targetWKTCoordinates =
                """
        (select jsonb_path_query_first(#{TARGET}#, '$."${geoQuery.geoproperty}"."$NGSILD_GEOPROPERTY_VALUE"[0]')->>'$JSONLD_VALUE')
        """.trimIndent()
            val georelQuery = GeoQueryParameter.Georel.prepareQuery(geoQuery.georel)

            return (
                if (georelQuery.first == GeoQueryParameter.Georel.NEAR_DISTANCE_MODIFIER)
                    """
            public.ST_Distance(
                cast('SRID=4326;${geoQuery.wktCoordinates.value}' as public.geography), 
                cast('SRID=4326;' || $targetWKTCoordinates as public.geography),
                false
            ) ${georelQuery.second} ${georelQuery.third}
            """.trimIndent()
                else
                    """
            public.ST_${georelQuery.first}(
                public.ST_GeomFromText('${geoQuery.wktCoordinates.value}'), 
                public.ST_GeomFromText($targetWKTCoordinates)
            ) 
            """.trimIndent()
                )
                .let {
                    if (target == null)
                        it.replace("#{TARGET}#", "entity_payload.payload")
                    else
                        it.replace("#{TARGET}#", "'" + JsonUtils.serializeObject(target.members) + "'")
                }
        }

    }
}
