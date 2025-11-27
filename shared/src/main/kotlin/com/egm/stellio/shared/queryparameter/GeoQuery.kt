package com.egm.stellio.shared.queryparameter

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_LOCATION_IRI
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.GeoUtils.parseGeometryToWKT
import com.egm.stellio.shared.util.GeoUtils.stringifyCoordinates
import com.egm.stellio.shared.util.HttpUtils.decode
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils

data class GeoQuery(
    val georel: String,
    val geometry: GeometryType,
    val coordinates: String,
    val wktCoordinates: WKTCoordinates,
    var geoproperty: ExpandedTerm = NGSILD_LOCATION_IRI
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

    fun buildSqlFilter(target: ExpandedEntity? = null): String {
        val targetWKTCoordinates =
            """
            (select jsonb_path_query_first(#{TARGET}#, '$."$geoproperty"."$NGSILD_GEOPROPERTY_VALUE"[0]')->>'$JSONLD_VALUE_KW')
            """.trimIndent()
        val georelQuery = Georel.prepareQuery(georel)

        return (
            if (georelQuery.first == Georel.NEAR_DISTANCE_MODIFIER)
                """
                public.ST_Distance(
                    cast('SRID=4326;${wktCoordinates.value}' as public.geography), 
                    cast('SRID=4326;' || $targetWKTCoordinates as public.geography),
                    false
                ) ${georelQuery.second} ${georelQuery.third}
                """.trimIndent()
            else
                """
                public.ST_${georelQuery.first}(
                    public.ST_GeomFromText($targetWKTCoordinates),
                    public.ST_GeomFromText('${wktCoordinates.value}')
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

    companion object {

        fun parseGeoQueryParameters(
            requestParams: Map<String, String>,
            contexts: List<String>
        ): Either<APIException, GeoQuery?> = either {
            val georel = requestParams[QueryParameter.GEOREL.key]?.decode()?.also {
                Georel.verify(it).bind()
            }
            val geometry = requestParams[QueryParameter.GEOMETRY.key]?.let {
                if (GeometryType.isSupportedType(it))
                    GeometryType.forType(it).right()
                else
                    BadRequestDataException("$it is not a recognized value for 'geometry' parameter").left()
            }?.bind()
            val coordinates = requestParams[QueryParameter.COORDINATES.key]?.decode()?.let {
                stringifyCoordinates(it)
            }
            val geoproperty = requestParams[QueryParameter.GEOPROPERTY.key]?.let {
                expandJsonLdTerm(it, contexts)
            } ?: NGSILD_LOCATION_IRI

            // if at least one parameter is provided, the three must be provided for the geoquery to be valid
            val notNullGeoParameters = listOfNotNull(georel, geometry, coordinates)
            if (notNullGeoParameters.isEmpty())
                null
            else if (georel == null || geometry == null || coordinates == null)
                BadRequestDataException(
                    "Missing at least one geo parameter between 'geometry', 'georel' and 'coordinates'"
                )
                    .left().bind<GeoQuery>()
            else
                GeoQuery(
                    georel = georel,
                    geometry = geometry,
                    coordinates = coordinates,
                    wktCoordinates = parseGeometryToWKT(geometry, coordinates).bind(),
                    geoproperty = geoproperty
                )
        }
    }
}
