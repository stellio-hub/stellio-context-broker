package com.egm.stellio.search.temporal.web

import arrow.core.raise.either
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.temporal.service.TemporalQueryService
import com.egm.stellio.search.temporal.util.TemporalEntityBuilder.wrapSingleValuesToList
import com.egm.stellio.search.temporal.util.TemporalPaginationUtils
import com.egm.stellio.search.temporal.util.composeTemporalEntitiesQueryFromPost
import com.egm.stellio.search.temporal.web.TemporalApiResponses.buildEntitiesTemporalResponse
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.filterPickAndOmit
import com.egm.stellio.shared.model.getRootAttributesToOmit
import com.egm.stellio.shared.model.getRootAttributesToPick
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entityOperations")
@Validated
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
        @AllowedParameters(
            implemented = [
                QP.LIMIT, QP.OFFSET, QP.COUNT, QP.OPTIONS
            ],
            notImplemented = [QP.LOCAL, QP.VIA]
        )
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val query = Query(requestBody.awaitFirst()).bind()

        val temporalEntitiesQuery =
            composeTemporalEntitiesQueryFromPost(
                applicationProperties.pagination,
                query,
                queryParams,
                contexts
            ).bind()

        val (temporalEntities, total) = temporalQueryService.queryTemporalEntities(
            temporalEntitiesQuery
        ).bind()

        val compactedEntities = compactEntities(temporalEntities, contexts)
            .filterPickAndOmit(
                temporalEntitiesQuery.entitiesQuery.pick.getRootAttributesToPick(),
                temporalEntitiesQuery.entitiesQuery.omit.getRootAttributesToOmit()
            )
            .wrapSingleValuesToList(temporalEntitiesQuery.temporalRepresentation)

        val range = TemporalPaginationUtils.calculateRangeFromEntities(
            compactedEntities,
            temporalEntitiesQuery
        )
        val rangedEntities = range?.let {
            TemporalPaginationUtils.filterEntitiesToRange(compactedEntities, it, temporalEntitiesQuery)
        } ?: compactedEntities

        buildEntitiesTemporalResponse(
            rangedEntities,
            total,
            "/ngsi-ld/v1/temporal/entities",
            temporalEntitiesQuery,
            queryParams,
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
