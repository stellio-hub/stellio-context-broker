package com.egm.stellio.search.web

import arrow.core.continuations.either
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.service.*
import com.egm.stellio.search.util.parseAndCheckQueryParams
import com.egm.stellio.search.util.prepareTemporalAttributes
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.getDatasetId
import com.egm.stellio.shared.model.parseToNgsiLdAttribute
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
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
    private val applicationProperties: ApplicationProperties,
    private val entityEventService: EntityEventService
) {

    /**
     * Implements 6.18.3.1 - Create Entity or Update Temporal Representation of Entity
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
            val entityWithMandatoryFiled = body.keepMandatoryFiled()
            val entityUri = entityWithMandatoryFiled[JSONLD_ID_TERM].toString().toUri()
            val entityNoExist = entityPayloadService.checkEntityExistence(entityUri, true).isRight()
            if (entityNoExist) {
                authorizationService.userCanCreateEntities(sub).bind()

                val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(entityWithMandatoryFiled, contexts)
                val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity()

                val attributesMetadata = ngsiLdEntity.prepareTemporalAttributes().bind()
                temporalEntityAttributeService.createEntityTemporalReferences(
                    ngsiLdEntity,
                    jsonLdEntity,
                    attributesMetadata,
                    sub.orNull()
                ).bind()
                authorizationService.createAdminLink(ngsiLdEntity.id, sub).bind()
                entityEventService.publishEntityCreateEvent(
                    sub.orNull(),
                    ngsiLdEntity.id,
                    ngsiLdEntity.types,
                    contexts
                )
            }
            authorizationService.userCanUpdateEntity(entityUri, sub).bind()

            val jsonLdAttributes = expandJsonLdFragment(body.removeMandatoryFiled(), contexts)
            var updateResults = UpdateResult(emptyList(), emptyList())
            jsonLdAttributes
                .forEach { attributeEntry ->
                    val attributeInstances = expandValueAsListOfMap(attributeEntry.value)
                    attributeInstances.forEach { attributeInstance ->
                        val datasetId = attributeInstance.getDatasetId()

                        val teaExistence =
                            temporalEntityAttributeService.hasAttribute(entityUri, attributeEntry.key, datasetId).bind()
                        val jsonLdAttribute = mapOf(attributeEntry.key to listOf(attributeInstance))
                        val ngsiLdAttributes = parseToNgsiLdAttribute(jsonLdAttribute)

                        val updateResult =
                            if (teaExistence) {
                                temporalEntityAttributeService.updateEntityAttributes(
                                    entityUri,
                                    ngsiLdAttributes,
                                    jsonLdAttribute,
                                    sub.orNull()
                                ).bind()
                            } else {
                                temporalEntityAttributeService.appendEntityAttributes(
                                    entityUri,
                                    ngsiLdAttributes,
                                    jsonLdAttribute,
                                    false,
                                    sub.orNull()
                                ).bind()
                            }

                        updateResults.mergeWith(updateResult)

                        if (updateResult.updated.isNotEmpty()) {
                            entityEventService.publishAttributeChangeEvents(
                                sub.orNull(),
                                entityUri,
                                jsonLdAttribute,
                                updateResult,
                                true,
                                contexts
                            )
                        }
                    }
                }

            if (entityNoExist && updateResults.notUpdated.isEmpty())
                ResponseEntity.status(HttpStatus.CREATED)
                    .location(URI("/ngsi-ld/v1/temporal/entities/$entityUri"))
                    .build<String>()
            else if (updateResults.notUpdated.isEmpty())
                ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
            else
                ResponseEntity.status(HttpStatus.MULTI_STATUS).body(updateResults)
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
