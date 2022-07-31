package com.egm.stellio.search.service

import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.codec.ClientCodecConfigurer
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import java.net.URI

@Component
class EntityService(
    applicationProperties: ApplicationProperties
) {

    private val consumer: (ClientCodecConfigurer) -> Unit =
        { configurer -> configurer.defaultCodecs().enableLoggingRequestDetails(true) }

    private val webClient by lazy {
        WebClient.builder()
            .exchangeStrategies(ExchangeStrategies.builder().codecs(consumer).build())
            .baseUrl(applicationProperties.entity.serviceUrl.toString())
            .build()
    }

    suspend fun getEntityById(entityId: URI, bearerToken: String): JsonLdEntity =
        webClient.get()
            .uri("/entities/$entityId")
            .header("Authorization", bearerToken)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { JsonLdUtils.expandJsonLdEntity(it) }
            .awaitFirst()
}
