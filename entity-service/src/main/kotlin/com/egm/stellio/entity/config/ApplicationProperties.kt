package com.egm.stellio.entity.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("application")
class ApplicationProperties(
    val pagination: Pagination
) {
    data class Pagination(
        val limitDefault: Int,
        val limitMax: Int
    )
}
