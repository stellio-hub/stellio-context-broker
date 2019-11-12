package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.config.ContextRegistryProperties
import com.egm.datahub.context.search.model.Entity
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@Component
class ContextRegistryService(
    private val ngsiLdParsingService: NgsiLdParsingService,
    contextRegistryProperties: ContextRegistryProperties
) {

    private var webClient = WebClient.create(contextRegistryProperties.url)

    fun getEntityById(entityId: String): Mono<Entity> {
        return webClient.get()
            .uri("/entities/$entityId")
            .retrieve()
            .bodyToMono(String::class.java)
            .map { ngsiLdParsingService.parse(it) }
    }
}