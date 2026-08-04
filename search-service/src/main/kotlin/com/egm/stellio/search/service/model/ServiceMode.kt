package com.egm.stellio.search.service.model

import com.fasterxml.jackson.annotation.JsonProperty

enum class ServiceMode(val key: String) {
    @JsonProperty("synchronous")
    SYNCHRONOUS("synchronous"),

    @JsonProperty("asynchronous")
    ASYNCHRONOUS("asynchronous");

    companion object {
        fun fromString(mode: String?): ServiceMode? =
            entries.firstOrNull { it.key == mode }
    }
}
