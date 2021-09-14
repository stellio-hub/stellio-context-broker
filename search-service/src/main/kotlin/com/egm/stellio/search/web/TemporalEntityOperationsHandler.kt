package com.egm.stellio.search.web

import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entityOperations")
class TemporalEntityOperationsHandler(
    private val queryService: QueryService,
    private val applicationProperties: ApplicationProperties,
    private val temporalEntityAttributeService: TemporalEntityAttributeService
) {

    /**
     * Partial implementation of 6.24.3.1 - Query Temporal Evolution of Entities With POST
     */
    @PostMapping("/query", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun queryEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val offset = params.getFirst(QUERY_PARAM_OFFSET)?.toIntOrNull() ?: 0
        val limit = params.getFirst(QUERY_PARAM_LIMIT)?.toIntOrNull() ?: applicationProperties.pagination.limitDefault
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val body = requestBody.awaitFirst()
        val params = JsonUtils.deserializeObject(body)
        val queryParams = LinkedMultiValueMap<String, String>()
        params.forEach {
            if (it.value is List<*>)
                queryParams.add(it.key, (it.value as List<*>).joinToString(","))
            else
                queryParams.add(it.key, it.value.toString())
        }

        val parsedParams = queryService.parseAndCheckQueryParams(queryParams, contextLink)
        val temporalEntities = queryService.queryTemporalEntities(
            limit,
            offset,
            parsedParams["ids"] as Set<URI>,
            parsedParams["types"] as Set<String>,
            parsedParams["temporalQuery"] as TemporalQuery,
            parsedParams["withTemporalValues"] as Boolean,
            contextLink
        )
        val temporalEntityCount = temporalEntityAttributeService.getCountForEntities(
            parsedParams["ids"] as Set<URI>,
            parsedParams["types"] as Set<String>,
            parsedParams["attrs"] as Set<String> ).awaitFirst()

        val prevAndNextLinks = PagingUtils.getPagingLinks(
            "/ngsi-ld/v1/temporal/entities",
            queryParams,
            temporalEntityCount,
            offset,
            limit
        )

        return PagingUtils.buildPaginationResponse(
            (serializeObject(temporalEntities.map { addContextsToEntity(it, listOf(contextLink), mediaType)})),
            temporalEntityCount,
            false,
            prevAndNextLinks,
            mediaType,
            contextLink
        )
    }
}
