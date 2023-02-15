package com.egm.stellio.search.web

import arrow.core.continuations.either
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.service.*
import com.egm.stellio.search.util.parseAndCheckQueryParams
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
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
import java.net.URI

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
     * Implements 6.18.3.1 - Create or Update (Upsert) Temporal Representation of Entity
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body)

        return either<APIException, ResponseEntity<*>> {
            val jsonLdTemporalEntity = expandJsonLdEntity(body, contexts)
            val entityUri = jsonLdTemporalEntity.id.toUri()
            val entityDoesNotExist = entityPayloadService.checkEntityExistence(entityUri, true).isRight()

            val jsonLdInstances = jsonLdTemporalEntity.getAttributes()
            jsonLdInstances.checkValidity().bind()
            val sortedJsonLdInstances = jsonLdInstances.sorted()

            if (entityDoesNotExist) {
                authorizationService.userCanCreateEntities(sub).bind()

                // create a view of the entity containing only the most recent instance of each attribute
                val jsonLdEntity = JsonLdEntity(
                    sortedJsonLdInstances
                        .keepFirstInstances()
                        .addCoreMembers(jsonLdTemporalEntity.id, jsonLdTemporalEntity.types),
                    contexts
                )
                val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity()

                entityPayloadService.createEntity(ngsiLdEntity, jsonLdEntity, sub.orNull()).bind()
                entityPayloadService.upsertAttributes(
                    entityUri,
                    sortedJsonLdInstances.removeFirstInstances(),
                    sub.orNull()
                ).bind()
                authorizationService.createAdminRight(entityUri, sub).bind()

                ResponseEntity.status(HttpStatus.CREATED)
                    .location(URI("/ngsi-ld/v1/temporal/entities/$entityUri"))
                    .build<String>()
            } else {
                authorizationService.userCanUpdateEntity(entityUri, sub).bind()
                entityPayloadService.upsertAttributes(
                    entityUri,
                    sortedJsonLdInstances,
                    sub.orNull()
                ).bind()

                ResponseEntity.status(HttpStatus.NO_CONTENT).build()
            }
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

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

            authorizationService.userCanUpdateEntity(entityUri, sub).bind()

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

                        attributeInstanceService.addAttributeInstance(
                            temporalEntityAttribute.id,
                            attributeEntry.key,
                            attributeInstance
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

            authorizationService.userCanReadEntity(entityUri, sub).bind()

            val temporalEntitiesQuery =
                parseAndCheckQueryParams(applicationProperties.pagination, requestParams, contextLink)

            val temporalEntity = queryService.queryTemporalEntity(
                entityUri,
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

            authorizationService.userCanUpdateEntity(entityUri, sub).bind()

            attributeInstanceService.deleteInstance(entityUri, expandedAttrId, instanceUri).bind()

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }
}
