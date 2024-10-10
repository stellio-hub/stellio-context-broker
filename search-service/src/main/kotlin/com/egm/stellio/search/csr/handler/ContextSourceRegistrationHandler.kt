package com.egm.stellio.search.csr.handler

import arrow.core.Either
import arrow.core.Option
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.ContextSourceRegistration.Companion.deserialize
import com.egm.stellio.search.csr.model.ContextSourceRegistration.Companion.unauthorizedMessage
import com.egm.stellio.search.csr.model.serialize
import com.egm.stellio.search.csr.service.ContextSourceRegistrationService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.QUERY_PARAM_OPTIONS
import com.egm.stellio.shared.util.QUERY_PARAM_OPTIONS_SYSATTRS_VALUE
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.parsePaginationParameters
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.util.toErrorResponse
import com.egm.stellio.shared.web.BaseHandler
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/csourceRegistrations")
class ContextSourceRegistrationHandler(
    private val applicationProperties: ApplicationProperties,
    private val contextSourceRegistrationService: ContextSourceRegistrationService
) : BaseHandler() {

    /**
     * Implements 6.8.3.1 - Create ContextSourceRegistration
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
        val sub = getSubFromSecurityContext()

        val contextSourceRegistration = deserialize(body, contexts).bind()

        contextSourceRegistrationService.create(contextSourceRegistration, sub).bind()

        ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/csourceRegistrations/${contextSourceRegistration.id}"))
            .build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.8.3.2 - Query ContextSourceRegistrations
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun get(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val sub = getSubFromSecurityContext()

        val includeSysAttrs = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
        val paginationQuery = parsePaginationParameters(
            params,
            applicationProperties.pagination.limitDefault,
            applicationProperties.pagination.limitMax
        ).bind()
        val contextSourceRegistrations = contextSourceRegistrationService.getContextSourceRegistrations(
            limit = paginationQuery.limit,
            offset = paginationQuery.offset,
            sub = sub,
        ).serialize(contexts, mediaType, includeSysAttrs)
        val contextSourceRegistrationsCount = contextSourceRegistrationService.getContextSourceRegistrationsCount(
            sub
        ).bind()

        buildQueryResponse(
            contextSourceRegistrations,
            contextSourceRegistrationsCount,
            "/ngsi-ld/v1/csourceRegistrations",
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
     * Implements 6.9.3.1 - Retrieve ContextSourceRegistration
     */
    @GetMapping("/{contextSourceRegistrationId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByURI(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable contextSourceRegistrationId: URI,
        @RequestParam options: String?
    ): ResponseEntity<*> = either {
        val includeSysAttrs = options == QUERY_PARAM_OPTIONS_SYSATTRS_VALUE
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val sub = getSubFromSecurityContext()
        checkIsAllowed(contextSourceRegistrationId, sub).bind()
        val contextSourceRegistration = contextSourceRegistrationService.getById(contextSourceRegistrationId).bind()

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(contextSourceRegistration.serialize(contexts, mediaType, includeSysAttrs))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.9.3.3 - Delete ContextSourceRegistration
     */
    @DeleteMapping("/{contextSourceRegistrationId}")
    suspend fun delete(@PathVariable contextSourceRegistrationId: URI): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        checkIsAllowed(contextSourceRegistrationId, sub).bind()

        contextSourceRegistrationService.delete(contextSourceRegistrationId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    private suspend fun checkIsAllowed(contextSourceRegistrationId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        contextSourceRegistrationService.isCreatorOf(contextSourceRegistrationId, sub)
            .flatMap {
                if (!it)
                    AccessDeniedException(
                        unauthorizedMessage(contextSourceRegistrationId)
                    ).left()
                else
                    Unit.right()
            }
}
