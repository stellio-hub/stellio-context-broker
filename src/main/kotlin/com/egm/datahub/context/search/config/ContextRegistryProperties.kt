package com.egm.datahub.context.search.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("application.context-registry")
data class ContextRegistryProperties(
    val url: String
)