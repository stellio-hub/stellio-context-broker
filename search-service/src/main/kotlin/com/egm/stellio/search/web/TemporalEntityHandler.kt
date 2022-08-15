package com.egm.stellio.search.web

import arrow.core.continuations.either
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.util.parseAndCheckQueryParams
import com.egm.stellio.shared.model.APIException
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
    private val entityPayloadService: EntityPayloadService,
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val queryService: QueryService,
    private val authorizationService: AuthorizationService,
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
        return either<APIException, ResponseEntity<*>> {
            val sub = getSubFromSecurityContext()
            val entityUri = entityId.toUri()

            entityPayloadService.checkEntityExistence(entityUri).bind()

            val body = requestBody.awaitFirst().deserializeAsMap()
            val contexts = checkAndGetContext(httpHeaders, body)
            val jsonLdAttributes = expandJsonLdFragment(body, contexts)
            val entityTypes = entityPayloadService.getTypes(entityUri).bind()

            authorizationService.checkUpdateAuthorized(
                entityUri,
                entityTypes,
                jsonLdAttributes,
                sub
            ).bind()

            jsonLdAttributes
                .forEach { attributeEntry ->
                    val attributeInstances = expandValueAsListOfMap(attributeEntry.value)
                    attributeInstances.forEach { attributeInstance ->
                        val datasetId = attributeInstance.getDatasetId()
                        val temporalEntityAttribute = temporalEntityAttributeService.getForEntityAndAttribute(
                            entityId.toUri(),
                            attributeEntry.key,
                            datasetId
                        ).bind()

                        val compactedAttributeName = compactTerm(attributeEntry.key, contexts)
                        attributeInstanceService.addAttributeInstance(
                            temporalEntityAttribute.id,
                            compactedAttributeName,
                            attributeInstance,
                            contexts
                        ).bind()
                    }
                }

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Partial implementation of 6.18.3.2 - Query Temporal Evolution of Entities
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getForEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        return either<APIException, ResponseEntity<*>> {
            val sub = getSubFromSecurityContext()
            val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
            val mediaType = getApplicableMediaType(httpHeaders)

            val temporalEntitiesQuery =
                parseAndCheckQueryParams(applicationProperties.pagination, params, contextLink, true)

            val accessRightFilter = authorizationService.computeAccessRightFilter(sub)
            val (temporalEntities, total) = queryService.queryTemporalEntities(
                temporalEntitiesQuery,
                contextLink,
                accessRightFilter
            ).bind()

            buildQueryResponse(
                serializeObject(temporalEntities.map { addContextsToEntity(it, listOf(contextLink), mediaType) }),
                total,
                "/ngsi-ld/v1/temporal/entities",
                temporalEntitiesQuery.queryParams,
                params,
                mediaType,
                contextLink
            )
        }.fold(
            { it.toErrorResponse() },
            { it }
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
            val entityUri = entityId.toUri()

            entityPayloadService.checkEntityExistence(entityUri).bind()

            val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
            val mediaType = getApplicableMediaType(httpHeaders)

            // FIXME this should no longer be needed since users are not entities anymore
            val entityTypes = entityPayloadService.getTypes(entityUri).bind()

            authorizationService.checkReadAuthorized(entityUri, entityTypes, sub).bind()

            val temporalEntitiesQuery =
                parseAndCheckQueryParams(applicationProperties.pagination, requestParams, contextLink)

            val temporalEntity = queryService.queryTemporalEntity(
                entityId.toUri(),
                temporalEntitiesQuery,
                contextLink
            ).bind()

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

            val entityTypes = entityPayloadService.getTypes(entityUri).bind()

            authorizationService.checkUpdateAuthorized(entityUri, entityTypes, expandedAttrId, sub).bind()

            attributeInstanceService.deleteEntityAttributeInstance(entityUri, expandedAttrId, instanceUri).bind()

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }
}
