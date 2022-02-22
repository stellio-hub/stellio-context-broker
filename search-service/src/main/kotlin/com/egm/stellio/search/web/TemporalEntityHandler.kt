package com.egm.stellio.search.web

import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.service.*
import com.egm.stellio.search.util.buildTemporalQuery
import com.egm.stellio.search.util.parseAndCheckQueryParams
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.getDatasetId
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandValueAsListOfMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.getSubFromSecurityContext
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
class TemporalEntityHandler(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val queryService: QueryService,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val applicationProperties: ApplicationProperties
) {

    /**
     * Implements 6.20.3.1 - Add attributes to Temporal Representation of Entity
     */
    @PostMapping("/{entityId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val canWriteEntity =
            entityAccessRightsService.canWriteEntity(sub, entityId.toUri()).awaitFirst()
        if (!canWriteEntity)
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)

        jsonLdAttributes
            .forEach { attributeEntry ->
                val attributeInstances = expandValueAsListOfMap(attributeEntry.value)
                attributeInstances.forEach { attributeInstance ->
                    val datasetId = attributeInstance.getDatasetId()
                    val temporalEntityAttributeUuid = temporalEntityAttributeService.getForEntityAndAttribute(
                        entityId.toUri(),
                        attributeEntry.key,
                        datasetId
                    ).awaitFirst()

                    val compactedAttributeName = compactTerm(attributeEntry.key, contexts)
                    attributeInstanceService.addAttributeInstance(
                        temporalEntityAttributeUuid,
                        compactedAttributeName,
                        attributeInstance,
                        contexts
                    ).awaitFirst()
                }
            }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }

    /**
     * Partial implementation of 6.18.3.2 - Query Temporal Evolution of Entities
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getForEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)

        val temporalEntitiesQuery = parseAndCheckQueryParams(applicationProperties.pagination, params, contextLink)

        val accessRightFilter = entityAccessRightsService.computeAccessRightFilter(sub)
        val (temporalEntities, total) = queryService.queryTemporalEntities(
            temporalEntitiesQuery,
            contextLink,
            accessRightFilter
        )

        val prevAndNextLinks = PagingUtils.getPagingLinks(
            "/ngsi-ld/v1/temporal/entities",
            params,
            total,
            temporalEntitiesQuery.offset,
            temporalEntitiesQuery.limit
        )

        return PagingUtils.buildPaginationResponse(
            serializeObject(temporalEntities.map { addContextsToEntity(it, listOf(contextLink), mediaType) }),
            total,
            temporalEntitiesQuery.count,
            prevAndNextLinks,
            mediaType,
            contextLink
        )
    }

    /**
     * Partial implementation of 6.19.3.1 (query parameters are not all supported)
     */
    @GetMapping("/{entityId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getForEntity(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)

        val withTemporalValues =
            hasValueInOptionsParam(
                Optional.ofNullable(params.getFirst(QUERY_PARAM_OPTIONS)),
                OptionsParamValue.TEMPORAL_VALUES
            )
        val withAudit = hasValueInOptionsParam(
            Optional.ofNullable(params.getFirst(QUERY_PARAM_OPTIONS)), OptionsParamValue.AUDIT
        )

        val temporalQuery = try {
            buildTemporalQuery(params, contextLink)
        } catch (e: BadRequestDataException) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse(e.message))
        }

        val canReadEntity =
            entityAccessRightsService.canReadEntity(sub, entityId.toUri()).awaitFirst()
        if (!canReadEntity)
            throw AccessDeniedException("User forbidden read access to entity $entityId")

        val temporalEntity = queryService.queryTemporalEntity(
            entityId.toUri(), temporalQuery, withTemporalValues, withAudit, contextLink
        )

        return buildGetSuccessResponse(mediaType, contextLink)
            .body(serializeObject(addContextsToEntity(temporalEntity, listOf(contextLink), mediaType)))
    }
}
