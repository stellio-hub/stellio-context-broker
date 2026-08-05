package com.egm.stellio.search.service.registration.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigDecimal

data class InputInformation(
    val type: InputInformationType,
    val required: Boolean = false,
    val properties: Map<String, InputInformation>? = null,
    val minimum: BigDecimal? = null,
    val maximum: BigDecimal? = null
)

enum class InputInformationType {
    @JsonProperty("object")
    OBJECT,

    @JsonProperty("array")
    ARRAY,

    @JsonProperty("string")
    STRING,

    @JsonProperty("number")
    NUMBER,

    @JsonProperty("integer")
    INTEGER,

    @JsonProperty("boolean")
    BOOLEAN,

    @JsonProperty("null")
    NULL
}
