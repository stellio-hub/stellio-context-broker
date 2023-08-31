package com.egm.stellio.subscription.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.oauth2.client.AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager
import org.springframework.security.oauth2.client.InMemoryReactiveOAuth2AuthorizedClientService
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository
import org.springframework.security.oauth2.client.web.reactive.function.client.ServerOAuth2AuthorizedClientExchangeFilterFunction
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.DefaultUriBuilderFactory

@Configuration
class WebClientConfig {

    @Bean
    @ConditionalOnProperty("application.authentication.enabled")
    fun webClient(
        clientRegistrations: ReactiveClientRegistrationRepository,
        @Value("\${subscription.entity-service-url}") entityServiceUrl: String
    ): WebClient {
        val clientService = InMemoryReactiveOAuth2AuthorizedClientService(clientRegistrations)
        val authorizedClientManager =
            AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager(clientRegistrations, clientService)
        val oauth = ServerOAuth2AuthorizedClientExchangeFilterFunction(authorizedClientManager)
        oauth.setDefaultClientRegistrationId("keycloak")
        val factory = DefaultUriBuilderFactory(entityServiceUrl)
        factory.encodingMode = DefaultUriBuilderFactory.EncodingMode.NONE
        return WebClient.builder()
            .uriBuilderFactory(factory)
            .baseUrl(entityServiceUrl)
            .filter(oauth)
            .build()
    }

    @Bean
    @ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
    fun webClientNoAuthentification(@Value("\${subscription.entity-service-url}") entityServiceUrl: String): WebClient {
        val factory = DefaultUriBuilderFactory(entityServiceUrl)
        factory.encodingMode = DefaultUriBuilderFactory.EncodingMode.NONE
        return WebClient.builder()
            .uriBuilderFactory(factory)
            .baseUrl(entityServiceUrl)
            .build()
    }
}
