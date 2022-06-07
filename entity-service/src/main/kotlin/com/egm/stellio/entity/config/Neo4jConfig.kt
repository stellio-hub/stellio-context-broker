package com.egm.stellio.entity.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.auditing.DateTimeProvider
import org.springframework.data.neo4j.config.EnableNeo4jAuditing
import java.time.Instant
import java.time.ZoneOffset
import java.util.Optional

@Configuration
@EnableNeo4jAuditing(
    dateTimeProviderRef = "fixedDateTimeProvider"
)
class Neo4jConfig {

    @Bean
    fun fixedDateTimeProvider(): DateTimeProvider =
        DateTimeProvider { Optional.of(Instant.now().atZone(ZoneOffset.UTC)) }
}
