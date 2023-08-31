package com.egm.stellio.search.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("search")
data class SearchProperties(
    val payloadMaxBodySize: Int
)
