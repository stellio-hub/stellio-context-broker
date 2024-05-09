package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.GEO_JSON_MEDIA_TYPE
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import org.springframework.http.MediaType

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
)

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
