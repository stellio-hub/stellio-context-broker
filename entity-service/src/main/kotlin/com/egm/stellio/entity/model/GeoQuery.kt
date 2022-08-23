package com.egm.stellio.entity.model

import com.egm.stellio.shared.util.JsonLdUtils

data class GeoQuery(
    val georel: String? = null,
    val geometry: String? = null,
    val coordinates: Any? = null,
    var geoproperty: String? = JsonLdUtils.NGSILD_LOCATION_PROPERTY
)
