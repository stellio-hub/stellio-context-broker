package com.egm.stellio.search.service

import com.egm.stellio.search.config.ContextRegistryProperties
import com.egm.stellio.search.util.NgsiLdParsingUtils
import org.springframework.http.codec.ClientCodecConfigurer
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@Component
class ContextRegistryService(
    contextRegistryProperties: ContextRegistryProperties
) {

    private final val consumer: (ClientCodecConfigurer) -> Unit = { configurer -> configurer.defaultCodecs().enableLoggingRequestDetails(true) }

    private var webClient = WebClient.builder()
        .exchangeStrategies(ExchangeStrategies.builder().codecs(consumer).build())
        .baseUrl(contextRegistryProperties.url)
        .build()

    fun getEntityById(entityId: String, bearerToken: String): Mono<Pair<Map<String, Any>, List<String>>> {
        return webClient.get()
            .uri("/entities/$entityId")
            .header("Authorization", bearerToken)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { NgsiLdParsingUtils.parseEntity(it) }
    }
}