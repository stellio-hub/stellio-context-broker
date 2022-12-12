package com.egm.stellio.subscription.web

import arrow.core.Either
import arrow.core.Option
import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.removeContextFromInput
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serialize
import com.egm.stellio.subscription.config.ApplicationProperties
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.model.toJson
import com.egm.stellio.subscription.service.SubscriptionEventService
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscription
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.net.URI
import java.util.Optional

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
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body)
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val parsedSubscription = parseSubscription(body, contexts).bind()
            checkSubscriptionNotExists(parsedSubscription).awaitFirst().bind()

            subscriptionService.create(parsedSubscription, sub).awaitFirst()
            subscriptionEventService.publishSubscriptionCreateEvent(
                sub.orNull(),
                parsedSubscription.id,
                contexts
            )

            ResponseEntity.status(HttpStatus.CREATED)
                .location(URI("/ngsi-ld/v1/subscriptions/${parsedSubscription.id}"))
                .build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.10.3.2 - Query Subscriptions
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getSubscriptions(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()
        val queryParams = parseAndCheckParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        )
        val subscriptions = subscriptionService.getSubscriptions(queryParams.limit, queryParams.offset, sub)
            .collectList().awaitFirst().toJson(contextLink, mediaType, queryParams.includeSysAttrs)
        val subscriptionsCount = subscriptionService.getSubscriptionsCount(sub).awaitFirst()

        return buildQueryResponse(
            subscriptions,
            subscriptionsCount,
            "/ngsi-ld/v1/subscriptions",
            queryParams,
            params,
            mediaType,
            contextLink
        )
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

        return either<APIException, ResponseEntity<*>> {
            val subscriptionIdUri = subscriptionId.toUri()
            checkSubscriptionExists(subscriptionIdUri).awaitFirst().bind()

            val sub = getSubFromSecurityContext()
            checkIsAllowed(subscriptionIdUri, sub).awaitFirst()
            val subscription = subscriptionService.getById(subscriptionIdUri).awaitFirst()

            prepareGetSuccessResponse(mediaType, contextLink)
                .body(subscription.toJson(contextLink, mediaType, includeSysAttrs))
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
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

        return either<APIException, ResponseEntity<*>> {
            val subscriptionIdUri = subscriptionId.toUri()
            checkSubscriptionExists(subscriptionIdUri).awaitFirst().bind()

            val sub = getSubFromSecurityContext()
            checkIsAllowed(subscriptionIdUri, sub).awaitFirst()
            val body = requestBody.awaitFirst().deserializeAsMap()
            val contexts = checkAndGetContext(httpHeaders, body)
            subscriptionService.update(subscriptionIdUri, body, contexts).awaitFirst()

            subscriptionEventService.publishSubscriptionUpdateEvent(
                sub.orNull(),
                subscriptionIdUri,
                removeContextFromInput(body).serialize(),
                contexts
            )
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.11.3.3 - Delete Subscription
     */
    @DeleteMapping("/{subscriptionId}")
    suspend fun delete(@PathVariable subscriptionId: String): ResponseEntity<*> {
        return either<APIException, ResponseEntity<*>> {
            val subscriptionIdUri = subscriptionId.toUri()
            checkSubscriptionExists(subscriptionIdUri).awaitFirst().bind()

            val sub = getSubFromSecurityContext()
            checkIsAllowed(subscriptionIdUri, sub).awaitFirst()
            subscriptionService.delete(subscriptionIdUri).awaitFirst()

            // TODO use JSON-LD contexts provided at creation time
            subscriptionEventService.publishSubscriptionDeleteEvent(
                sub.orNull(),
                subscriptionIdUri,
                listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    @DeleteMapping("/", "")
    suspend fun handleMissingIdOnDelete(): ResponseEntity<*> =
        missingPathErrorResponse("Missing id when trying to delete a subscription")

    private fun checkSubscriptionExists(subscriptionId: URI): Mono<Either<APIException, URI>> =
        subscriptionService.exists(subscriptionId)
            .flatMap {
                if (!it)
                    Mono.just(ResourceNotFoundException(subscriptionNotFoundMessage(subscriptionId)).left())
                else
                    Mono.just(subscriptionId.right())
            }

    private fun checkSubscriptionNotExists(subscription: Subscription): Mono<Either<APIException, Subscription>> =
        subscriptionService.exists(subscription.id)
            .flatMap {
                if (it)
                    Mono.just(AlreadyExistsException(subscriptionAlreadyExistsMessage(subscription.id)).left())
                else Mono.just(subscription.right())
            }

    private fun checkIsAllowed(subscriptionId: URI, sub: Option<Sub>): Mono<URI> =
        subscriptionService.isCreatorOf(subscriptionId, sub)
            .flatMap {
                if (!it)
                    Mono.error(AccessDeniedException(subscriptionUnauthorizedMessage(subscriptionId)))
                else
                    Mono.just(subscriptionId)
            }
}
