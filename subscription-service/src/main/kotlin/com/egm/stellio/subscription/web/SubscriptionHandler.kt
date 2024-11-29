package com.egm.stellio.subscription.web

import arrow.core.Either
import arrow.core.Option
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.PaginationQuery.Companion.parsePaginationParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
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
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(notImplemented = [QP.VIA])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
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
        @AllowedParameters(
            implemented = [QP.OPTIONS, QP.LIMIT, QP.OFFSET, QP.COUNT],
            notImplemented = [QP.VIA]
        )
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val sub = getSubFromSecurityContext()

        val includeSysAttrs = params.getOrDefault(QueryParameter.OPTIONS.key, emptyList())
            .contains(OptionsValue.SYS_ATTRS.value)
        val paginationQuery = parsePaginationParameters(
            params,
            applicationProperties.pagination.limitDefault,
            applicationProperties.pagination.limitMax
        ).bind()
        val subscriptions = subscriptionService.getSubscriptions(paginationQuery.limit, paginationQuery.offset, sub)
            .serialize(contexts, mediaType, includeSysAttrs)
        val subscriptionsCount = subscriptionService.getSubscriptionsCount(sub).bind()

        buildQueryResponse(
            subscriptions,
            subscriptionsCount,
            "/ngsi-ld/v1/subscriptions",
            paginationQuery,
            params,
            mediaType,
            contexts
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
        @RequestParam options: Optional<String>,
        @AllowedParameters(implemented = [QP.OPTIONS], notImplemented = [QP.VIA])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val includeSysAttrs = options.filter { it.contains(OptionsValue.SYS_ATTRS.value) }.isPresent
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        checkSubscriptionExists(subscriptionId).bind()

        val sub = getSubFromSecurityContext()
        checkIsAllowed(subscriptionId, sub).bind()
        val subscription = subscriptionService.getById(subscriptionId)

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(subscription.serialize(contexts, mediaType, includeSysAttrs))
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
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(notImplemented = [QP.VIA])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        checkSubscriptionExists(subscriptionId).bind()

        val sub = getSubFromSecurityContext()
        checkIsAllowed(subscriptionId, sub).bind()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
        subscriptionService.update(subscriptionId, body, contexts).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.11.3.3 - Delete Subscription
     */
    @DeleteMapping("/{subscriptionId}")
    suspend fun delete(
        @PathVariable subscriptionId: URI,
        @AllowedParameters(notImplemented = [QP.VIA])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        checkSubscriptionExists(subscriptionId).bind()

        val sub = getSubFromSecurityContext()
        checkIsAllowed(subscriptionId, sub).bind()
        subscriptionService.delete(subscriptionId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

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
