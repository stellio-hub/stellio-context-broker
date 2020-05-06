package com.egm.stellio.search.service

import com.egm.stellio.search.config.EntityServiceProperties
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import org.springframework.http.codec.ClientCodecConfigurer
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@Component
class EntityService(
    entityServiceProperties: EntityServiceProperties
) {

    private final val consumer: (ClientCodecConfigurer) -> Unit = { configurer -> configurer.defaultCodecs().enableLoggingRequestDetails(true) }

    private var webClient = WebClient.builder()
        .exchangeStrategies(ExchangeStrategies.builder().codecs(consumer).build())
        .baseUrl(entityServiceProperties.url)
        .build()

    fun getEntityById(entityId: String, bearerToken: String): Mono<ExpandedEntity> =
        webClient.get()
            .uri("/entities/$entityId")
            .header("Authorization", bearerToken)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { NgsiLdParsingUtils.parseEntity(it) }
}
