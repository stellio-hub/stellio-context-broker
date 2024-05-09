package com.egm.stellio.search.web

import arrow.core.raise.either
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.model.Query
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.util.composeTemporalEntitiesQueryFromPostRequest
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entityOperations")
class TemporalEntityOperationsHandler(
    private val queryService: QueryService,
    private val authorizationService: AuthorizationService,
    private val applicationProperties: ApplicationProperties
) {

    /**
     * Implementation of 6.24.3.1 - Query Temporal Evolution of Entities via POST
     */
    @PostMapping("/query", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun queryEntitiesViaPost(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val query = Query(requestBody.awaitFirst()).bind()

        val temporalEntitiesQuery =
            composeTemporalEntitiesQueryFromPostRequest(
                applicationProperties.pagination,
                query,
                params,
                contexts
            ).bind()

        val accessRightFilter = authorizationService.computeAccessRightFilter(sub)

        val (temporalEntities, total) = queryService.queryTemporalEntities(
            temporalEntitiesQuery,
            accessRightFilter
        ).bind()

        val compactedEntities = compactEntities(temporalEntities, contexts)

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
            .copy(languageFilter = query.lang)

        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            total,
            "/ngsi-ld/v1/temporal/entities",
            temporalEntitiesQuery.entitiesQuery.paginationQuery,
            params,
            mediaType,
            contexts
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
