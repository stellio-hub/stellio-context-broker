package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.ErrorMessages.Subscription.entitiesRetrievalFailedMessage
import com.egm.stellio.shared.util.ErrorMessages.Subscription.entitiesRetrievalTimeoutMessage
import com.egm.stellio.shared.util.JsonUtils.deserializeListOfObjects
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.NGSILD_TENANT_HEADER
import com.egm.stellio.shared.util.encode
import com.egm.stellio.subscription.config.SubscriptionProperties
import com.egm.stellio.subscription.model.Endpoint.AcceptType
import com.egm.stellio.subscription.model.NotificationParams
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.security.oauth2.client.web.reactive.function.client.ServerOAuth2AuthorizedClientExchangeFilterFunction
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.net.URI
import java.util.concurrent.TimeoutException

@Component
class CoreAPIService(
    private val webClient: WebClient,
    subscriptionProperties: SubscriptionProperties
) {
    private val linkedEntitiesProperties = subscriptionProperties.linkedEntities

    private val linkedEntitiesRetry = Retry
        .backoff(linkedEntitiesProperties.retryMaxAttempts, linkedEntitiesProperties.retryMinBackoff)
        .maxBackoff(linkedEntitiesProperties.retryMaxBackoff)
        .filter { it.isTransientFailure() }
        .onRetryExhaustedThrow { _, signal -> signal.failure() }

    suspend fun getEntities(
        tenantName: String,
        paramRequest: String,
        contextLink: String
    ): Either<APIException, List<CompactedEntity>> =
        webClient.get()
            .uri("/ngsi-ld/v1/entities$paramRequest")
            .header(HttpHeaders.LINK, contextLink)
            .header(NGSILD_TENANT_HEADER, tenantName)
            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(tenantName))
            .retrieve()
            .bodyToMono<String>()
            .timeout(linkedEntitiesProperties.timeout)
            .map<Either<APIException, List<CompactedEntity>>> { deserializeListOfObjects(it).right() }
            .onErrorResume { Mono.just(it.toSearchServiceError("the entities matching $paramRequest").left()) }
            .awaitFirst()

    /**
     * Retrieves an entity with its linked entities, with a single attempt by default.
     * A per-attempt timeout covers both the connection and the response. With [withRetry], transient failures
     * (see [isTransientFailure]) are retried with a backoff, without blocking the calling thread.
     */
    suspend fun retrieveLinkedEntities(
        tenantName: String,
        entityId: URI,
        notificationParams: NotificationParams,
        contextLink: String,
        withRetry: Boolean = false
    ): Either<APIException, List<CompactedEntity>> {
        val queryParams = prepareQueryParamsFromNotificationParams(notificationParams)
        // GeoJSON conversion of linked entities is done by NotificationService.callSubscriber
        val accept = notificationParams.endpoint.accept.takeIf { it != AcceptType.GEOJSON } ?: AcceptType.JSON
        return webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId?$queryParams")
            .header(HttpHeaders.LINK, contextLink)
            .header(HttpHeaders.ACCEPT, accept.accept)
            .header(NGSILD_TENANT_HEADER, tenantName)
            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(tenantName))
            .retrieve()
            .bodyToMono<String>()
            .timeout(linkedEntitiesProperties.timeout)
            .let { if (withRetry) it.retryWhen(linkedEntitiesRetry) else it }
            .map<Either<APIException, List<CompactedEntity>>> {
                // flaky way to know if the response is a single entity or a list of entities
                if (it.startsWith("["))
                    deserializeListOfObjects(it).right()
                else listOf(deserializeObject(it)).right()
            }
            .onErrorResume { Mono.just(it.toSearchServiceError("the linked entities of $entityId").left()) }
            .awaitFirst()
    }

    private fun prepareQueryParamsFromNotificationParams(
        notificationParams: NotificationParams
    ): String {
        val params = StringBuilder()

        if (!notificationParams.attributes.isNullOrEmpty()) {
            val attrsParam = notificationParams.attributes.joinToString(",") { it.encode() }
            params.append("&${QueryParameter.ATTRS.key}=$attrsParam")
        }
        if (!notificationParams.pick.isNullOrEmpty()) {
            val pickParam = notificationParams.pick.joinToString(",") { it.encode() }
            params.append("&${QueryParameter.PICK.key}=$pickParam")
        }
        if (!notificationParams.omit.isNullOrEmpty()) {
            val omitParam = notificationParams.omit.joinToString(",") { it.encode() }
            params.append("&${QueryParameter.OMIT.key}=$omitParam")
        }
        val optionsParam =
            if (notificationParams.sysAttrs)
                "${OptionsValue.SYS_ATTRS.value},${notificationParams.format.format}"
            else
                notificationParams.format.format
        params.append("&${QueryParameter.OPTIONS.key}=$optionsParam")

        if (notificationParams.join != null && notificationParams.join != NotificationParams.JoinType.NONE) {
            params.append("&${QueryParameter.JOIN.key}=${notificationParams.join.join}")
            notificationParams.joinLevel?.let {
                params.append("&${QueryParameter.JOIN_LEVEL.key}=$it")
            }
        }

        return if (params.isEmpty()) "" else "${params.substring(1)}"
    }
}

private fun Throwable.isTransientFailure(): Boolean =
    this is TimeoutException ||
        this is WebClientRequestException ||
        this is WebClientResponseException && this.statusCode.is5xxServerError

private fun Throwable.toSearchServiceError(target: String): APIException =
    when {
        this is TimeoutException -> GatewayTimeoutException(entitiesRetrievalTimeoutMessage(target))
        this.isTransientFailure() -> BadGatewayException(entitiesRetrievalFailedMessage(target, this.message))
        else -> InternalErrorException(entitiesRetrievalFailedMessage(target, this.message))
    }

fun APIException.isTransientFailure(): Boolean =
    this is GatewayTimeoutException || this is BadGatewayException
