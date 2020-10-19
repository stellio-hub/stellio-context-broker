package com.egm.stellio.subscription.web

import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.PagingUtils.SUBSCRIPTION_QUERY_PAGING_LIMIT
import com.egm.stellio.shared.util.PagingUtils.getSubscriptionsPagingLinks
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.model.toJson
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscription
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscriptionUpdate
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.net.URI
import java.util.*

@RestController
@RequestMapping("/ngsi-ld/v1/subscriptions")
class SubscriptionHandler(
    private val subscriptionService: SubscriptionService
) {

    /**
     * Implements 6.10.3.1 - Create Subscription
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val body = requestBody.awaitFirst()
        val parsedSubscription = parseSubscription(body, checkAndGetContext(httpHeaders, body))
        checkSubscriptionNotExists(parsedSubscription).awaitFirst()

        val userId = extractSubjectOrEmpty().awaitFirst()
        subscriptionService.create(parsedSubscription, userId).awaitFirst()

        return ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/subscriptions/${parsedSubscription.id}"))
            .build<String>()
    }

    /**
     * Implements 6.10.3.2 - Query Subscriptions
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getSubscriptions(
        @RequestParam(required = false, defaultValue = "1") page: Int,
        @RequestParam(required = false, defaultValue = SUBSCRIPTION_QUERY_PAGING_LIMIT.toString()) limit: Int,
        @RequestParam options: Optional<String>
    ): ResponseEntity<*> {
        val includeSysAttrs = options.filter { it.contains("sysAttrs") }.isPresent
        if (limit <= 0 || page <= 0)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse("Page number and Limit must be greater than zero"))

        val userId = extractSubjectOrEmpty().awaitFirst()
        val subscriptions = subscriptionService.getSubscriptions(limit, (page - 1) * limit, userId)
            .collectList().awaitFirst().toJson(includeSysAttrs)

        val prevAndNextLinks = getSubscriptionsPagingLinks(
            subscriptionService.getSubscriptionsCount(userId).awaitFirst(),
            page,
            limit
        )

        return if (prevAndNextLinks.first != null && prevAndNextLinks.second != null)
            ResponseEntity.status(HttpStatus.OK).header("Link", prevAndNextLinks.first)
                .header("Link", prevAndNextLinks.second).body(subscriptions)
        else if (prevAndNextLinks.first != null)
            ResponseEntity.status(HttpStatus.OK).header("Link", prevAndNextLinks.first).body(subscriptions)
        else if (prevAndNextLinks.second != null)
            ResponseEntity.status(HttpStatus.OK).header("Link", prevAndNextLinks.second).body(subscriptions)
        else
            ResponseEntity.status(HttpStatus.OK).body(subscriptions)
    }

    /**
     * Implements 6.11.3.1 - Retrieve Subscription
     */
    @GetMapping("/{subscriptionId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByURI(
        @PathVariable subscriptionId: String,
        @RequestParam options: Optional<String>
    ): ResponseEntity<*> {
        val includeSysAttrs = options.filter { it.contains("sysAttrs") }.isPresent
        val subscriptionIdUri = subscriptionId.toUri()
        checkSubscriptionExists(subscriptionIdUri).awaitFirst()

        val userId = extractSubjectOrEmpty().awaitFirst()
        checkIsAllowed(subscriptionIdUri, userId).awaitFirst()
        val subscription = subscriptionService.getById(subscriptionIdUri).awaitFirst()

        return ResponseEntity.status(HttpStatus.OK).body(subscription.toJson(includeSysAttrs))
    }

    /**
     * Implements 6.11.3.2 - Update Subscription
     */
    @PatchMapping(
        "/{subscriptionId}",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun update(
        @PathVariable subscriptionId: String,
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val subscriptionIdUri = subscriptionId.toUri()
        checkSubscriptionExists(subscriptionIdUri).awaitFirst()

        val userId = extractSubjectOrEmpty().awaitFirst()
        checkIsAllowed(subscriptionIdUri, userId).awaitFirst()
        val body = requestBody.awaitFirst()
        val parsedInput = parseSubscriptionUpdate(body, checkAndGetContext(httpHeaders, body))
        subscriptionService.update(subscriptionIdUri, parsedInput).awaitFirst()

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }

    /**
     * Implements 6.11.3.3 - Delete Subscription
     */
    @DeleteMapping("/{subscriptionId}")
    suspend fun delete(@PathVariable subscriptionId: String): ResponseEntity<*> {
        val subscriptionIdUri = subscriptionId.toUri()
        checkSubscriptionExists(subscriptionIdUri).awaitFirst()

        val userId = extractSubjectOrEmpty().awaitFirst()
        checkIsAllowed(subscriptionIdUri, userId).awaitFirst()
        subscriptionService.delete(subscriptionIdUri).awaitFirst()

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }

    private fun checkSubscriptionExists(subscriptionId: URI): Mono<URI> =
        subscriptionService.exists(subscriptionId)
            .flatMap {
                if (!it)
                    Mono.error(ResourceNotFoundException(subscriptionNotFoundMessage(subscriptionId)))
                else
                    Mono.just(subscriptionId)
            }

    private fun checkSubscriptionNotExists(subscription: Subscription): Mono<Subscription> =
        subscriptionService.exists(subscription.id)
            .flatMap {
                if (it)
                    Mono.error(AlreadyExistsException(subscriptionAlreadyExistsMessage(subscription.id)))
                else
                    Mono.just(subscription)
            }

    private fun checkIsAllowed(subscriptionId: URI, userSub: String): Mono<URI> =
        subscriptionService.isCreatorOf(subscriptionId, userSub)
            .flatMap {
                if (!it)
                    Mono.error(AccessDeniedException(subscriptionUnauthorizedMessage(subscriptionId)))
                else
                    Mono.just(subscriptionId)
            }
}
