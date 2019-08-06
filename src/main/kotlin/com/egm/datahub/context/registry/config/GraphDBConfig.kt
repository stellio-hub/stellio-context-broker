package com.egm.datahub.context.registry.config

import com.egm.datahub.context.registry.config.properties.GraphdbProperties
import org.eclipse.rdf4j.repository.http.HTTPRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class GraphDBConfig(
        private val graphdbProperties: GraphdbProperties
) {
    @Bean
    fun httpRepository(): HTTPRepository {
        val repository = HTTPRepository(graphdbProperties.url)
        repository.setUsernameAndPassword(graphdbProperties.username, graphdbProperties.password)
        return repository
    }
}