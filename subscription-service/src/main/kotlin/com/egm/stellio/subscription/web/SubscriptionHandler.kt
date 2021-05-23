package com.egm.stellio.subscription.web

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.removeContextFromInput
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.PagingUtils.getPagingLinks
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import com.egm.stellio.subscription.config.ApplicationProperties
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.model.toJson
import com.egm.stellio.subscription.service.SubscriptionEventService
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscription
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscriptionUpdate
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
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
    private val applicationProperties: ApplicationProperties,
    private val subscriptionService: SubscriptionService,
    private val subscriptionEventService: SubscriptionEventService
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
        val contexts = checkAndGetContext(httpHeaders, body)
        val parsedSubscription = parseSubscription(body, contexts)
        checkSubscriptionNotExists(parsedSubscription).awaitFirst()

        val userId = extractSubjectOrEmpty().awaitFirst()
        subscriptionService.create(parsedSubscription, userId).awaitFirst()
        subscriptionEventService.publishSubscriptionEvent(
            EntityCreateEvent(
                parsedSubscription.id,
                removeContextFromInput(body),
                contexts
            )
        )
        return ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/subscriptions/${parsedSubscription.id}"))
            .build<String>()
    }

    /**
     * Implements 6.10.3.2 - Query Subscriptions
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getSubscriptions(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>,
        @RequestParam options: Optional<String>
    ): ResponseEntity<*> {
        val page = params.getFirst(PAGE_PARAM_ID)?.toIntOrNull() ?: 1
        val limit = params.getFirst(LIMIT_PARAM_ID)?.toIntOrNull() ?: applicationProperties.pagination.limitDefault
        val includeSysAttrs = options.filter { it.contains("sysAttrs") }.isPresent
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        if (limit <= 0 || page <= 0)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse("Page number and Limit must be greater than zero"))

        if (limit > applicationProperties.pagination.limitMax)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(
                    BadRequestDataResponse(
                        "You asked for $limit results, " +
                            "but the supported maximum limit is ${applicationProperties.pagination.limitMax}"
                    )
                )

        val userId = extractSubjectOrEmpty().awaitFirst()
        val subscriptions = subscriptionService.getSubscriptions(limit, (page - 1) * limit, userId)
            .collectList().awaitFirst().toJson(contextLink, mediaType, includeSysAttrs)

        val prevAndNextLinks = getPagingLinks(
            "/ngsi-ld/v1/subscriptions",
            params,
            subscriptionService.getSubscriptionsCount(userId).awaitFirst(),
            page,
            limit
        )

        return PagingUtils.buildPaginationResponse(subscriptions, prevAndNextLinks, mediaType, contextLink)
    }

    /**
     * Implements 6.11.3.1 - Retrieve Subscription
     */
    @GetMapping("/{subscriptionId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByURI(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subscriptionId: String,
        @RequestParam options: Optional<String>
    ): ResponseEntity<*> {
        val includeSysAttrs = options.filter { it.contains("sysAttrs") }.isPresent
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val subscriptionIdUri = subscriptionId.toUri()
        checkSubscriptionExists(subscriptionIdUri).awaitFirst()

        val userId = extractSubjectOrEmpty().awaitFirst()
        checkIsAllowed(subscriptionIdUri, userId).awaitFirst()
        val subscription = subscriptionService.getById(subscriptionIdUri).awaitFirst()

        return buildGetSuccessResponse(mediaType, contextLink)
            .body(subscription.toJson(contextLink, mediaType, includeSysAttrs))
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
        val context = checkAndGetContext(httpHeaders, body)
        val parsedInput = parseSubscriptionUpdate(body, context)
        subscriptionService.update(subscriptionIdUri, parsedInput).awaitFirst()

        subscriptionEventService.publishSubscriptionEvent(
            EntityUpdateEvent(
                subscriptionIdUri,
                removeContextFromInput(body),
                serializeObject(subscriptionService.getById(subscriptionIdUri).awaitFirst()),
                context
            )
        )
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

        subscriptionEventService.publishSubscriptionEvent(
            EntityDeleteEvent(
                subscriptionIdUri,
                listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
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
