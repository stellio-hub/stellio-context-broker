package com.egm.stellio.subscription.web

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.web.BaseHandler
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.model.serialize
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
    private val subscriptionService: SubscriptionService
) : BaseHandler() {

    /**
     * Implements 6.10.3.1 - Create Subscription
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body).bind()
        val sub = getSubFromSecurityContext()

        val subscription = parseSubscription(body, contexts).bind()
        checkSubscriptionNotExists(subscription).bind()

        subscriptionService.create(subscription, sub).bind()

        ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/subscriptions/${subscription.id}"))
            .build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.10.3.2 - Query Subscriptions
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getSubscriptions(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        val queryParams = parseQueryParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        ).bind()
        val subscriptions = subscriptionService.getSubscriptions(queryParams.limit, queryParams.offset, sub)
            .serialize(contextLink, mediaType, queryParams.includeSysAttrs)
        val subscriptionsCount = subscriptionService.getSubscriptionsCount(sub).bind()

        buildQueryResponse(
            subscriptions,
            subscriptionsCount,
            "/ngsi-ld/v1/subscriptions",
            queryParams,
            params,
            mediaType,
            contextLink
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.11.3.1 - Retrieve Subscription
     */
    @GetMapping("/{subscriptionId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByURI(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subscriptionId: URI,
        @RequestParam options: Optional<String>
    ): ResponseEntity<*> = either {
        val includeSysAttrs = options.filter { it.contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE) }.isPresent
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders)

        checkSubscriptionExists(subscriptionId).bind()

        val sub = getSubFromSecurityContext()
        checkIsAllowed(subscriptionId, sub).bind()
        val subscription = subscriptionService.getById(subscriptionId)

        prepareGetSuccessResponse(mediaType, contextLink)
            .body(subscription.serialize(contextLink, mediaType, includeSysAttrs))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Return the contexts associated to a given subscription.
     *
     * It is used when a notification is sent using JSON media type and the associated context contains more than
     * one link.
     */
    @GetMapping("/{subscriptionId}/context", produces = [MediaType.APPLICATION_JSON_VALUE])
    suspend fun getSubscriptionContext(@PathVariable subscriptionId: URI): ResponseEntity<*> = either {
        val contexts = subscriptionService.getContextsForSubscription(subscriptionId).bind()

        ResponseEntity.ok(serializeObject(mapOf(JSONLD_CONTEXT to contexts)))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.11.3.2 - Update Subscription
     */
    @PatchMapping(
        "/{subscriptionId}",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun update(
        @PathVariable subscriptionId: URI,
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        checkSubscriptionExists(subscriptionId).bind()

        val sub = getSubFromSecurityContext()
        checkIsAllowed(subscriptionId, sub).bind()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body).bind()
        subscriptionService.update(subscriptionId, body, contexts).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping("/", "")
    fun handleMissingIdOnUpdate(): ResponseEntity<*> =
        missingPathErrorResponse("Missing id when trying to update a subscription")

    /**
     * Implements 6.11.3.3 - Delete Subscription
     */
    @DeleteMapping("/{subscriptionId}")
    suspend fun delete(@PathVariable subscriptionId: URI): ResponseEntity<*> = either {
        checkSubscriptionExists(subscriptionId).bind()

        val sub = getSubFromSecurityContext()
        checkIsAllowed(subscriptionId, sub).bind()
        subscriptionService.delete(subscriptionId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/", "")
    fun handleMissingIdOnDelete(): ResponseEntity<*> =
        missingPathErrorResponse("Missing id when trying to delete a subscription")

    private suspend fun checkSubscriptionExists(subscriptionId: URI): Either<APIException, Unit> =
        subscriptionService.exists(subscriptionId)
            .flatMap {
                if (!it)
                    ResourceNotFoundException(subscriptionNotFoundMessage(subscriptionId)).left()
                else
                    Unit.right()
            }

    private suspend fun checkSubscriptionNotExists(subscription: Subscription): Either<APIException, Unit> =
        subscriptionService.exists(subscription.id)
            .flatMap {
                if (it)
                    AlreadyExistsException(subscriptionAlreadyExistsMessage(subscription.id)).left()
                else Unit.right()
            }

    private suspend fun checkIsAllowed(subscriptionId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        subscriptionService.isCreatorOf(subscriptionId, sub)
            .flatMap {
                if (!it)
                    AccessDeniedException(subscriptionUnauthorizedMessage(subscriptionId)).left()
                else
                    Unit.right()
            }
}
