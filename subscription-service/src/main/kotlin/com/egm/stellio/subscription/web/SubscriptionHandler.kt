package com.egm.stellio.subscription.web

import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscription
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscriptionUpdate
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.shared.util.PagingUtils.getSubscriptionsPagingLinks
import com.egm.stellio.shared.util.PagingUtils.SUBSCRIPTION_QUERY_PAGING_LIMIT
import com.egm.stellio.shared.util.ApiUtils.serializeObject
import com.egm.stellio.shared.web.extractJwT
import com.egm.stellio.subscription.model.Subscription
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import org.springframework.web.reactive.function.server.queryParamOrNull
import reactor.core.publisher.Mono
import java.net.URI

@Component
class SubscriptionHandler(
    private val subscriptionService: SubscriptionService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Implements 6.10.3.1 - Create Subscription
     */
    fun create(req: ServerRequest): Mono<ServerResponse> {
        return req.bodyToMono<String>()
                .map {
                    val context = NgsiLdParsingUtils.getContextOrThrowError(it)
                    parseSubscription(it, context)
                }
                .flatMap {
                    checkSubscriptionNotExists(it)
                }
                .zipWith(extractJwT())
                .flatMap { subscriptionAndSubject ->
                    subscriptionService.create(subscriptionAndSubject.t1, subscriptionAndSubject.t2.subject).map { subscriptionAndSubject.t1 }
                }
                .flatMap {
                    created(URI("/ngsi-ld/v1/subscriptions/${it.id}")).build()
                }
    }

    /**
     * Implements 6.10.3.2 - Query Subscriptions
     */
    fun getSubscriptions(req: ServerRequest): Mono<ServerResponse> {
        val pageNumber = req.queryParamOrNull("page")?.toInt() ?: 1
        val limit = req.queryParamOrNull("limit")?.toInt() ?: SUBSCRIPTION_QUERY_PAGING_LIMIT

        return if (limit <= 0 || pageNumber <= 0)
            badRequest().contentType(MediaType.APPLICATION_JSON).bodyValue(BadRequestDataResponse("Page number and Limit must be greater than zero"))

        else extractJwT()
            .flatMap {
                subscriptionService.getSubscriptionsCount(it.subject).flatMap { count ->
                    Mono.just(Pair(count, it.subject))
                }
            }
            .flatMap { subscriptionsCountAndSubject ->
                subscriptionService.getSubscriptions(limit, (pageNumber - 1) * limit, subscriptionsCountAndSubject.second).collectList().flatMap {
                    Mono.just(Pair(subscriptionsCountAndSubject.first, it))
                }
            }
            .map {
                val prevLink = getSubscriptionsPagingLinks(it.first, pageNumber, limit).first
                val nextLink = getSubscriptionsPagingLinks(it.first, pageNumber, limit).second
                Triple(serializeObject(it.second), prevLink, nextLink)
            }
            .flatMap {
                if (it.second != null && it.third != null)
                    ok().header("Link", it.second).header("Link", it.third).body(BodyInserters.fromValue(it.first))
                else if (it.second != null)
                    ok().header("Link", it.second).body(BodyInserters.fromValue(it.first))
                else if (it.third != null)
                    ok().header("Link", it.third).body(BodyInserters.fromValue(it.first))
                else
                    ok().body(BodyInserters.fromValue(it.first))
            }
    }

    /**
     * Implements 6.11.3.1 - Retrieve Subscription
     */
    fun getByURI(req: ServerRequest): Mono<ServerResponse> {
        val subscriptionId = req.pathVariable("subscriptionId")
        return checkSubscriptionExists(subscriptionId)
            .flatMap {
                extractJwT()
            }
            .flatMap {
                checkIsAllowed(subscriptionId, it.subject)
            }
            .flatMap {
                subscriptionService.getById(subscriptionId)
            }
            .flatMap {
                ok().body(BodyInserters.fromValue(serializeObject(it)))
            }
    }

    /**
     * Implements 6.11.3.2 - Update Subscription
     */
    fun update(req: ServerRequest): Mono<ServerResponse> {
        val subscriptionId = req.pathVariable("subscriptionId")
        return checkSubscriptionExists(subscriptionId)
            .flatMap {
                extractJwT()
            }
            .flatMap {
                checkIsAllowed(subscriptionId, it.subject)
            }
            .flatMap {
                req.bodyToMono<String>()
            }
            .flatMap {
                val parsedInput = parseSubscriptionUpdate(it)
                subscriptionService.update(subscriptionId, parsedInput)
            }
            .flatMap {
                noContent().build()
            }
    }

    /**
     * Implements 6.11.3.3 - Delete Subscription
     */
    fun delete(req: ServerRequest): Mono<ServerResponse> {
        val subscriptionId = req.pathVariable("subscriptionId")

        return checkSubscriptionExists(subscriptionId)
            .flatMap {
                extractJwT()
            }
            .flatMap {
                checkIsAllowed(subscriptionId, it.subject)
            }
            .flatMap {
                subscriptionService.delete(subscriptionId)
            }
            .flatMap {
                noContent().build()
            }
    }

    private fun checkSubscriptionExists(subscriptionId: String): Mono<String> =
        subscriptionService.exists(subscriptionId)
            .flatMap {
                if (!it)
                    Mono.error(ResourceNotFoundException("Could not find a subscription with id $subscriptionId"))
                else
                    Mono.just(subscriptionId)
            }

    private fun checkSubscriptionNotExists(subscription: Subscription): Mono<Subscription> =
        subscriptionService.exists(subscription.id)
            .flatMap {
                if (it)
                    Mono.error(AlreadyExistsException("A subscription with id ${subscription.id} already exists"))
                else
                    Mono.just(subscription)
            }

    private fun checkIsAllowed(subscriptionId: String, userSub: String): Mono<String> =
        subscriptionService.isCreatorOf(subscriptionId, userSub)
            .flatMap {
                if (!it)
                    Mono.error(AccessDeniedException("User is not authorized to access subscription $subscriptionId"))
                else
                    Mono.just(subscriptionId)
            }
}
