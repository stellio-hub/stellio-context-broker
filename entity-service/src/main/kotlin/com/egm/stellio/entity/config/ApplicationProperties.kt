package com.egm.stellio.entity.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("application")
data class ApplicationProperties(
    val pagination: Pagination,
    val eventsThreadPool: EventsThreadPool = EventsThreadPool()
) {
    data class Pagination(
        val limitDefault: Int,
        val limitMax: Int
    )

    data class EventsThreadPool(
        val corePoolSize: Int = 10,
        val maxPoolSize: Int = 100
    )
}
