package com.egm.stellio.search.web

import arrow.core.continuations.either
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityAccessRightsService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.util.parseAndCheckQueryParams
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.getDatasetId
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandValueAsListOfMap
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

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

        val canWriteEntity = entityAccessRightsService.canWriteEntity(sub, entityId.toUri())
        if (canWriteEntity.isLeft())
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst().deserializeAsMap()
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
        if (temporalEntitiesQuery.queryParams.types.isEmpty() &&
            temporalEntitiesQuery.queryParams.attrs.isEmpty()
        ) throw BadRequestDataException("Either type or attrs need to be present in request parameters")

        val accessRightFilter = entityAccessRightsService.computeAccessRightFilter(sub)
        val (temporalEntities, total) = queryService.queryTemporalEntities(
            temporalEntitiesQuery,
            contextLink,
            accessRightFilter
        )

        return buildQueryResponse(
            serializeObject(temporalEntities.map { addContextsToEntity(it, listOf(contextLink), mediaType) }),
            total,
            "/ngsi-ld/v1/temporal/entities",
            temporalEntitiesQuery.queryParams,
            params,
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
        @RequestParam requestParams: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        return either<APIException, ResponseEntity<*>> {
            val sub = getSubFromSecurityContext()
            val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
            val mediaType = getApplicableMediaType(httpHeaders)

            val temporalEntitiesQuery =
                parseAndCheckQueryParams(applicationProperties.pagination, requestParams, contextLink)

            entityAccessRightsService.canReadEntity(sub, entityId.toUri()).bind()

            val temporalEntity = queryService.queryTemporalEntity(
                entityId.toUri(),
                temporalEntitiesQuery.temporalQuery,
                temporalEntitiesQuery.withTemporalValues,
                temporalEntitiesQuery.withAudit,
                contextLink
            )

            prepareGetSuccessResponse(mediaType, contextLink)
                .body(serializeObject(addContextsToEntity(temporalEntity, listOf(contextLink), mediaType)))
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.22.3.2 - Delete Attribute instance from Temporal Representation of an Entity
     */
    @DeleteMapping("/{entityId}/attrs/{attrId}/{instanceId}")
    suspend fun deleteAttributeInstanceTemporal(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @PathVariable attrId: String,
        @PathVariable instanceId: String
    ): ResponseEntity<*> {
        return either<APIException, ResponseEntity<*>> {
            val sub = getSubFromSecurityContext()
            val entityUri = entityId.toUri()
            val instanceUri = instanceId.toUri()
            val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders))
            val expandedAttrId = JsonLdUtils.expandJsonLdTerm(attrId, contexts)

            temporalEntityAttributeService.checkEntityAndAttributeExistence(entityUri, expandedAttrId).bind()

            entityAccessRightsService.canWriteEntity(sub, entityUri).bind()

            attributeInstanceService.deleteEntityAttributeInstance(entityUri, expandedAttrId, instanceUri).bind()

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }
}
