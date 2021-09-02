package com.egm.stellio.search.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.net.URI

@ConstructorBinding
@ConfigurationProperties("application")
data class dockerApplicationProperties(
    val entity: Entity,
    val authentication: Authentication
) {
    data class Authentication(
        val enabled: Boolean
    )

    data class Entity(
        val serviceUrl: URI,
        val storePayloads: Boolean
    )
}
