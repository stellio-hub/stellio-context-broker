package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.config.ContextRegistryProperties
import com.egm.datahub.context.search.model.Entity
import org.springframework.http.codec.ClientCodecConfigurer
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@Component
class ContextRegistryService(
    private val ngsiLdParsingService: NgsiLdParsingService,
    contextRegistryProperties: ContextRegistryProperties
) {

    private final val consumer: (ClientCodecConfigurer) -> Unit = { configurer -> configurer.defaultCodecs().enableLoggingRequestDetails(true) }

    private var webClient = WebClient.builder()
        .exchangeStrategies(ExchangeStrategies.builder().codecs(consumer).build())
        .baseUrl(contextRegistryProperties.url)
        .build()

    fun getEntityById(entityId: String, bearerToken: String): Mono<Entity> {
        return webClient.get()
            .uri("/entities/$entityId")
            .header("Authorization", bearerToken)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { ngsiLdParsingService.parse(it) }
    }
}