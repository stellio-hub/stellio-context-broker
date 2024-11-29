package com.egm.stellio.shared.model

import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.GEO_JSON_MEDIA_TYPE
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_TERM
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
            requestParams: MultiValueMap<String, String>,
            acceptMediaType: MediaType
        ): NgsiLdDataRepresentation {
            val optionsParam = requestParams.getOrDefault(QueryParameter.OPTIONS.key, emptyList())
            val includeSysAttrs = optionsParam.contains(OptionsValue.SYS_ATTRS.value)
            val attributeRepresentation = optionsParam.contains(OptionsValue.KEY_VALUES.value)
                .let { if (it) AttributeRepresentation.SIMPLIFIED else AttributeRepresentation.NORMALIZED }
            val languageFilter = requestParams.getFirst(QueryParameter.LANG.key)
            val entityRepresentation = EntityRepresentation.forMediaType(acceptMediaType)
            val geometryProperty =
                if (entityRepresentation == EntityRepresentation.GEO_JSON)
                    requestParams.getFirst(QueryParameter.GEOMETRY_PROPERTY.key) ?: NGSILD_LOCATION_TERM
                else null
            val timeproperty = requestParams.getFirst("timeproperty")

            return NgsiLdDataRepresentation(
                entityRepresentation,
                attributeRepresentation,
                includeSysAttrs,
                languageFilter,
                geometryProperty,
                timeproperty
            )
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
