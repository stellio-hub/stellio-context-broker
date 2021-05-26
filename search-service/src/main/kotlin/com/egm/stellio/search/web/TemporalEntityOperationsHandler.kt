package com.egm.stellio.search.web

import com.egm.stellio.search.util.QueryUtils
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.util.*

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entityOperations")
class TemporalEntityOperationsHandler(
    private val queryUtils: QueryUtils
) {

    /**
     * Partial implementation of 6.24.3.1 - Query Temporal Evolution of Entities With POST
     */
    @PostMapping("/query", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun queryEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val body = requestBody.awaitFirst()
        val params = JsonUtils.deserializeObject(body)
        val queryParams = LinkedMultiValueMap<String, String>()
        params.forEach { queryParams.add(it.key, it.value.toString()) }

        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val withTemporalValues = hasValueInOptionsParam(
            Optional.ofNullable(queryParams.getFirst("options")), OptionsParamValue.TEMPORAL_VALUES
        )
        val ids = parseRequestParameter(queryParams.getFirst(QUERY_PARAM_ID)).map { it.toUri() }.toSet()
        val types = parseAndExpandRequestParameter(queryParams.getFirst(QUERY_PARAM_TYPE), contextLink)
        val temporalQuery = try {
            buildTemporalQuery(queryParams, contextLink)
        } catch (e: BadRequestDataException) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse(e.message))
        }
        if (types.isEmpty() && temporalQuery.expandedAttrs.isEmpty())
            throw BadRequestDataException("Either type or attrs need to be present in request parameters")

        val temporalEntities = queryUtils.queryTemporalEntities(
            ids, types, temporalQuery, withTemporalValues, listOf(contextLink)
        ).awaitFirst()

        return buildGetSuccessResponse(mediaType, contextLink)
            .body(serializeObject(temporalEntities.map { addContextsToEntity(it, listOf(contextLink), mediaType) }))
    }
}
