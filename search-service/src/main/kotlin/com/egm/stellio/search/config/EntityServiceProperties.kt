package com.egm.stellio.search.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("application.entity-service")
data class EntityServiceProperties(
    val url: String
)