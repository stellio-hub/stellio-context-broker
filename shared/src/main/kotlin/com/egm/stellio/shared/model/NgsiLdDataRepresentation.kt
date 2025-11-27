package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.queryparameter.FormatValue
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.ApiUtils.GEO_JSON_MEDIA_TYPE
import com.egm.stellio.shared.util.ApiUtils.JSON_LD_MEDIA_TYPE
import org.springframework.http.MediaType
import org.springframework.util.MultiValueMap

/**
 * Wrapper data class used to convey possible NGSI-LD Data Representations for entities as defined in 4.5
 */
data class NgsiLdDataRepresentation(
    val entityRepresentation: EntityRepresentation,
    val attributeRepresentation: AttributeRepresentation,
    val includeSysAttrs: Boolean,
    val languageFilter: String? = null,
    // In the case of GeoJSON Entity representation,
    // this parameter indicates which GeoProperty to use for the toplevel geometry field
    val geometryProperty: String? = null,
    // In the case of a temporal property, do not remove this property if sysAttrs is not asked
    val timeproperty: String? = null
) {
    companion object {
        fun parseRepresentations(
            queryParams: MultiValueMap<String, String>,
            acceptMediaType: MediaType
        ): Either<APIException, NgsiLdDataRepresentation> = either {
            val optionsParam = queryParams.getOrDefault(QueryParameter.OPTIONS.key, emptyList())
                .flatMap { it.split(",") }
                .map { OptionsValue.fromString(it).bind() }
            val formatParam = queryParams.getFirst(QueryParameter.FORMAT.key)?.let {
                FormatValue.fromString(it).bind()
            }
            val attributeRepresentation = when {
                formatParam == FormatValue.KEY_VALUES || formatParam == FormatValue.SIMPLIFIED ->
                    AttributeRepresentation.SIMPLIFIED
                formatParam == FormatValue.NORMALIZED ->
                    AttributeRepresentation.NORMALIZED
                optionsParam.contains(OptionsValue.KEY_VALUES) || optionsParam.contains(OptionsValue.SIMPLIFIED) ->
                    AttributeRepresentation.SIMPLIFIED
                else -> AttributeRepresentation.NORMALIZED
            }
            val includeSysAttrs = optionsParam.contains(OptionsValue.SYS_ATTRS)
            val languageFilter = queryParams.getFirst(QueryParameter.LANG.key)
            val entityRepresentation = EntityRepresentation.forMediaType(acceptMediaType)
            val geometryProperty =
                if (entityRepresentation == EntityRepresentation.GEO_JSON)
                    queryParams.getFirst(QueryParameter.GEOMETRY_PROPERTY.key) ?: NGSILD_LOCATION_TERM
                else null
            val timeproperty = queryParams.getFirst(QueryParameter.TIMEPROPERTY.key)

            return NgsiLdDataRepresentation(
                entityRepresentation,
                attributeRepresentation,
                includeSysAttrs,
                languageFilter,
                geometryProperty,
                timeproperty
            ).right()
        }
    }
}

enum class AttributeRepresentation {
    NORMALIZED,
    SIMPLIFIED
}

enum class EntityRepresentation {
    JSON_LD,
    JSON,
    GEO_JSON;

    companion object {
        fun forMediaType(mediaType: MediaType): EntityRepresentation =
            when (mediaType) {
                JSON_LD_MEDIA_TYPE -> JSON_LD
                GEO_JSON_MEDIA_TYPE -> GEO_JSON
                else -> JSON
            }
    }
}
