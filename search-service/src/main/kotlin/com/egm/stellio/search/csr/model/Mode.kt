package com.egm.stellio.search.csr.model

import com.fasterxml.jackson.annotation.JsonProperty

enum class Mode(val key: String) {
    @JsonProperty("inclusive")
    INCLUSIVE("inclusive"),

    @JsonProperty("exclusive")
    EXCLUSIVE("exclusive"),

    @JsonProperty("redirect")
    REDIRECT("redirect"),

    @JsonProperty("auxiliary")
    AUXILIARY("auxiliary");
    companion object {
        fun fromString(mode: String?): Mode =
            Mode.entries.firstOrNull { it.key == mode } ?: INCLUSIVE
    }
}
