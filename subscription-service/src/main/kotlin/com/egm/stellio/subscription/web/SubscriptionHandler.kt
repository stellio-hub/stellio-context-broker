package com.egm.stellio.subscription.web

import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.ApiUtils.serializeObject
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.PagingUtils.SUBSCRIPTION_QUERY_PAGING_LIMIT
import com.egm.stellio.shared.util.PagingUtils.getSubscriptionsPagingLinks
import com.egm.stellio.shared.web.extractJwT
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscription
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscriptionUpdate
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/subscriptions")
class SubscriptionHandler(
    private val subscriptionService: SubscriptionService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Implements 6.10.3.1 - Create Subscription
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun create(@RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {
        return body
            .map {
                val context = NgsiLdParsingUtils.getContextOrThrowError(it)
                parseSubscription(it, context)
            }
            .flatMap {
                checkSubscriptionNotExists(it)
            }
            .zipWith(extractJwT())
            .flatMap { subscriptionAndSubject ->
                subscriptionService.create(subscriptionAndSubject.t1, subscriptionAndSubject.t2.subject)
                    .map { subscriptionAndSubject.t1 }
            }
            .map {
                ResponseEntity.status(HttpStatus.CREATED).location(URI("/ngsi-ld/v1/subscriptions/${it.id}"))
                    .build<String>()
            }
    }

    /**
     * Implements 6.10.3.2 - Query Subscriptions
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun getSubscriptions(
        @RequestParam(required = false, defaultValue = "1") page: Int,
        @RequestParam(required = false, defaultValue = SUBSCRIPTION_QUERY_PAGING_LIMIT.toString()) limit: Int
    ): Mono<ResponseEntity<*>> {
        return if (limit <= 0 || page <= 0)
            ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse("Page number and Limit must be greater than zero"))
                .toMono()
        else extractJwT()
            .flatMap {
                subscriptionService.getSubscriptionsCount(it.subject).flatMap { count ->
                    Mono.just(Pair(count, it.subject))
                }
            }
            .flatMap { subscriptionsCountAndSubject ->
                subscriptionService.getSubscriptions(limit, (page - 1) * limit, subscriptionsCountAndSubject.second)
                    .collectList().flatMap {
                        Mono.just(Pair(subscriptionsCountAndSubject.first, it))
                    }
            }
            .map {
                val prevLink = getSubscriptionsPagingLinks(it.first, page, limit).first
                val nextLink = getSubscriptionsPagingLinks(it.first, page, limit).second
                Triple(serializeObject(it.second), prevLink, nextLink)
            }
            .map {
                if (it.second != null && it.third != null)
                    ResponseEntity.status(HttpStatus.OK).header("Link", it.second).header("Link", it.third)
                        .body(it.first)
                else if (it.second != null)
                    ResponseEntity.status(HttpStatus.OK).header("Link", it.second).body(it.first)
                else if (it.third != null)
                    ResponseEntity.status(HttpStatus.OK).header("Link", it.third).body(it.first)
                else
                    ResponseEntity.status(HttpStatus.OK).body(it.first)
            }
    }

    /**
     * Implements 6.11.3.1 - Retrieve Subscription
     */
    @GetMapping("/{subscriptionId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun getByURI(@PathVariable subscriptionId: String): Mono<ResponseEntity<*>> {
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
            .map {
                ResponseEntity.status(HttpStatus.OK).body(serializeObject(it))
            }
    }

    /**
     * Implements 6.11.3.2 - Update Subscription
     */
    @PatchMapping(
        "/{subscriptionId}",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    fun update(@PathVariable subscriptionId: String, @RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {
        return checkSubscriptionExists(subscriptionId)
            .flatMap {
                extractJwT()
            }
            .flatMap {
                checkIsAllowed(subscriptionId, it.subject)
            }
            .flatMap {
                body
            }
            .flatMap {
                val parsedInput = parseSubscriptionUpdate(it)
                subscriptionService.update(subscriptionId, parsedInput)
            }
            .map {
                ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
            }
    }

    /**
     * Implements 6.11.3.3 - Delete Subscription
     */
    @DeleteMapping("/{subscriptionId}")
    fun delete(@PathVariable subscriptionId: String): Mono<ResponseEntity<*>> {
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
            .map {
                ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
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
