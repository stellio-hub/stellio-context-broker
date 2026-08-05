package com.egm.stellio.search.service.registration.web

import arrow.core.raise.either
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.service.registration.model.ServiceRegistration.Companion.deserialize
import com.egm.stellio.search.service.registration.model.ServiceRegistrationFilters
import com.egm.stellio.search.service.registration.model.serialize
import com.egm.stellio.search.service.registration.service.ServiceRegistrationService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.PaginationQuery.Companion.parsePaginationParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.web.BaseHandler
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.validation.annotation.Validated
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

@RestController
@RequestMapping("/ngsi-ld/v1/serviceRegistrations")
@Validated
class ServiceRegistrationHandler(
    private val applicationProperties: ApplicationProperties,
    private val serviceRegistrationService: ServiceRegistrationService,
    private val authorizationService: AuthorizationService
) : BaseHandler() {
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
        val serviceRegistration = deserialize(body, contexts).bind()

        serviceRegistrationService.create(serviceRegistration).bind()

        ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/serviceRegistrations/${serviceRegistration.id}"))
            .build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun query(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(
            implemented = [
                QP.ID, QP.TYPE, QP.OPTIONS, QP.COUNT, QP.OFFSET, QP.LIMIT
            ],
            notImplemented = [
                QP.ID_PATTERN
            ]
        )
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val filters = ServiceRegistrationFilters.fromQueryParameters(queryParams, contexts).bind()
        val paginationQuery = parsePaginationParameters(
            queryParams,
            applicationProperties.pagination.limitDefault,
            applicationProperties.pagination.limitMax
        ).bind()
        val includeSysAttrs = queryParams.getOrDefault(QueryParameter.OPTIONS.key, emptyList())
            .contains(OptionsValue.SYS_ATTRS.value)

        val registrations = serviceRegistrationService.getServiceRegistrations(
            filters,
            paginationQuery.limit,
            paginationQuery.offset
        ).serialize(contexts, mediaType, includeSysAttrs)
        val registrationsCount = serviceRegistrationService.getServiceRegistrationsCount(filters).bind()

        buildQueryResponse(
            registrations,
            registrationsCount,
            "/ngsi-ld/v1/serviceRegistrations",
            paginationQuery,
            queryParams,
            mediaType,
            contexts
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @GetMapping("/{serviceRegistrationId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun retrieve(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable serviceRegistrationId: URI,
        @AllowedParameters(implemented = [QP.OPTIONS])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val includeSysAttrs = queryParams.getFirst(QP.OPTIONS.key)
            ?.contains(OptionsValue.SYS_ATTRS.value)
            ?: false
        val serviceRegistration = serviceRegistrationService.getById(serviceRegistrationId).bind()

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(serviceRegistration.serialize(contexts, mediaType, includeSysAttrs))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping(
        "/{serviceRegistrationId}",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun update(
        @PathVariable serviceRegistrationId: URI,
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        val currentRegistration = serviceRegistrationService.getById(serviceRegistrationId).bind()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
        val updatedRegistration = currentRegistration.mergeWithFragment(body, contexts).bind()

        serviceRegistrationService.upsert(updatedRegistration).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/{serviceRegistrationId}")
    suspend fun delete(
        @PathVariable serviceRegistrationId: URI,
        @AllowedParameters
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        authorizationService.userIsAdmin().bind()
        serviceRegistrationService.delete(serviceRegistrationId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
