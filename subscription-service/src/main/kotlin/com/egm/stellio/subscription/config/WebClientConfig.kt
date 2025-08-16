package com.egm.stellio.subscription.config

import com.egm.stellio.shared.config.ApplicationProperties
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.security.oauth2.client.AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager
import org.springframework.security.oauth2.client.InMemoryReactiveOAuth2AuthorizedClientService
import org.springframework.security.oauth2.client.ReactiveOAuth2AuthorizedClientManager
import org.springframework.security.oauth2.client.registration.ClientRegistration
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository
import org.springframework.security.oauth2.client.web.reactive.function.client.ServerOAuth2AuthorizedClientExchangeFilterFunction
import org.springframework.security.oauth2.core.AuthorizationGrantType
import org.springframework.security.oauth2.core.ClientAuthenticationMethod
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.DefaultUriBuilderFactory

/**
 * Configures a multi-tenant WebClient used to call the search-service.
 *
 * When authentication is enabled:
 * - A ClientRegistration is built for each tenant declared in ApplicationProperties.
 *   The token endpoint is derived from the tenant issuer: <issuer>/protocol/openid-connect/token.
 *   Client credentials (client-id/secret) are specific to each tenant and must be provided in
 *   application.tenants[...].clientId and application.tenants[...].clientSecret.
 * - A ReactiveOAuth2AuthorizedClientManager is created and plugged into the WebClient through
 *   ServerOAuth2AuthorizedClientExchangeFilterFunction, so that bearer tokens are obtained and
 *   automatically attached to outgoing requests.
 * - No default client registration is set at the WebClient level; instead, callers (CoreAPIService)
 *   must select the tenant per request by setting the attribute
 *   ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(tenantName). This
 *   instructs Spring Security to pick the matching tenant registration and fetch the correct token.
 *
 * When authentication is disablede:
 * - A plain WebClient without the OAuth2 filter is exposed.
 *
 */
@Configuration
class WebClientConfig {

    /**
     * Creates one OAuth2 ClientRegistration per tenant.
     *
     * - Registration id = tenant name (e.g., urn:ngsi-ld:tenant:default).
     * - tokenUri is derived from the tenant issuer by appending "/protocol/openid-connect/token".
     * - Uses client_credentials grant with CLIENT_SECRET_BASIC auth method.
     * - Client credentials (client-id/secret) are per-tenant and are read from application properties.
     */
    @Bean
    @Primary
    @ConditionalOnProperty("application.authentication.enabled")
    fun clientRegistrationRepository(
        applicationProperties: ApplicationProperties
    ): ReactiveClientRegistrationRepository {
        val registrations = applicationProperties.tenants.map { tenant ->
            // derive token endpoint from issuer (Keycloak default path)
            val tokenUri = tenant.issuer.trimEnd('/') + "/protocol/openid-connect/token"
            ClientRegistration.withRegistrationId(tenant.name)
                .tokenUri(tokenUri)
                .authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS)
                .clientId(tenant.clientId ?: error("Missing clientId for tenant ${tenant.name}"))
                .clientSecret(tenant.clientSecret ?: error("Missing clientSecret for tenant ${tenant.name}"))
                .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
                .build()
        }
        return InMemoryReactiveClientRegistrationRepository(registrations)
    }

    /**
     * Exposes a ReactiveOAuth2AuthorizedClientManager that performs token acquisition and
     * manages authorized clients in-memory. It is used by the WebClient OAuth2 filter to
     * transparently exchange client credentials for an access token when a request specifies
     * a clientRegistrationId (the tenant name in our case).
     */
    @Bean
    @ConditionalOnProperty("application.authentication.enabled")
    fun reactiveOAuth2AuthorizedClientManager(
        clientRegistrations: ReactiveClientRegistrationRepository
    ): ReactiveOAuth2AuthorizedClientManager {
        val clientService = InMemoryReactiveOAuth2AuthorizedClientService(clientRegistrations)
        return AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager(clientRegistrations, clientService)
    }

    /**
     * WebClient configured with the OAuth2 filter using the provided authorizedClientManager.
     *
     * Notes:
     * - No default clientRegistrationId is set at builder time; the caller must set it per request:
     *   .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(tenantName))
     * - Base URL comes from subscription.entity-service-url.
     */
    @Bean
    @ConditionalOnProperty("application.authentication.enabled")
    fun webClient(
        authorizedClientManager: ReactiveOAuth2AuthorizedClientManager,
        @Value("\${subscription.entity-service-url}") entityServiceUrl: String
    ): WebClient {
        val oauth = ServerOAuth2AuthorizedClientExchangeFilterFunction(authorizedClientManager)
        val factory = DefaultUriBuilderFactory(entityServiceUrl)
        factory.encodingMode = DefaultUriBuilderFactory.EncodingMode.NONE
        return WebClient.builder()
            .uriBuilderFactory(factory)
            .baseUrl(entityServiceUrl)
            .filter(oauth)
            .build()
    }

    /**
     * Plain WebClient used when authentication is disabled. No OAuth2 filter is installed, but
     * the same base URL and UriBuilderFactory are used.
     */
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
