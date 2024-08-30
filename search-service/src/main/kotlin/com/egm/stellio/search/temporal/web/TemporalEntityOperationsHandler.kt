package com.egm.stellio.search.temporal.web

import arrow.core.raise.either
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.temporal.service.TemporalQueryService
import com.egm.stellio.search.temporal.util.composeTemporalEntitiesQueryFromPostRequest
import com.egm.stellio.search.temporal.web.TemporalApiResponses.buildEntitiesTemporalResponse
import com.egm.stellio.shared.config.ApplicationProperties
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
    private val temporalQueryService: TemporalQueryService,
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

        val (temporalEntities, total, range) = temporalQueryService.queryTemporalEntities(
            temporalEntitiesQuery,
            sub.getOrNull()
        ).bind()

        val compactedEntities = compactEntities(temporalEntities, contexts)

        buildEntitiesTemporalResponse(
            compactedEntities,
            total,
            "/ngsi-ld/v1/temporal/entities",
            temporalEntitiesQuery,
            params,
            mediaType,
            contexts,
            range,
            query.lang
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
