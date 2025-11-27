package com.egm.stellio.search.authorization.subject.web

import arrow.core.raise.either
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromGet
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.NgsiLdDataRepresentation.Companion.parseRepresentations
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.ApiResponses.buildQueryResponse
import com.egm.stellio.shared.util.ApiUtils.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.ApiUtils.getApplicableMediaType
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.getAuthzContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.web.BaseHandler
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@Validated
@RestController
@RequestMapping("/ngsi-ld/v1/auth/subjects")
class SubjectHandler(
    private val applicationProperties: ApplicationProperties,
    private val authorizationService: AuthorizationService
) : BaseHandler() {

    @GetMapping("/groups", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getGroupsMemberships(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(implemented = [QP.COUNT, QP.OFFSET, QP.LIMIT])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType).bind()

        val contexts = getAuthzContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts).bind()
        val entitiesQuery = composeEntitiesQueryFromGet(
            applicationProperties.pagination,
            params,
            contexts
        ).bind()

        val (count, entities) =
            authorizationService.getGroupsMemberships(
                entitiesQuery.paginationQuery.offset,
                entitiesQuery.paginationQuery.limit,
                contexts
            ).bind()

        if (count == -1) {
            return@either ResponseEntity.status(HttpStatus.NO_CONTENT).build()
        }

        val compactedEntities = compactEntities(entities, contexts)

        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            count,
            "/ngsi-ld/v1/auth/subjects/groups",
            entitiesQuery.paginationQuery,
            params,
            mediaType,
            contexts
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @GetMapping("/users", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getUsers(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(implemented = [QP.COUNT, QP.OFFSET, QP.LIMIT])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType).bind()

        authorizationService.userIsAdmin().bind()

        val contexts = getAuthzContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts).bind()
        val entitiesQuery = composeEntitiesQueryFromGet(
            applicationProperties.pagination,
            params,
            contexts
        ).bind()

        val (count, entities) =
            authorizationService.getUsers(
                entitiesQuery.paginationQuery.offset,
                entitiesQuery.paginationQuery.limit,
                contexts
            ).bind()

        if (count == -1) {
            return@either ResponseEntity.status(HttpStatus.NO_CONTENT).build()
        }

        val compactedEntities = compactEntities(entities, contexts)

        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            count,
            "/ngsi-ld/v1/auth/subjects/users",
            entitiesQuery.paginationQuery,
            params,
            mediaType,
            contexts
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
