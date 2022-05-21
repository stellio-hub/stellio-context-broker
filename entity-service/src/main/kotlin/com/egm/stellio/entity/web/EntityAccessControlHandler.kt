package com.egm.stellio.entity.web

import arrow.core.*
import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.ApplicationProperties
import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.updateResultFromDetailedResult
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS_TERMS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.COMPOUND_AUTHZ_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.NGSILD_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.compactTerms
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import kotlin.collections.flatten

@RestController
@RequestMapping("/ngsi-ld/v1/entityAccessControl")
class EntityAccessControlHandler(
    private val applicationProperties: ApplicationProperties,
    private val entityService: EntityService,
    private val authorizationService: AuthorizationService,
    private val entityEventService: EntityEventService,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @GetMapping("/entities", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getAuthorizedEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        val queryParams = parseAndCheckParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        )

        if (queryParams.q != null && !ALL_IAM_RIGHTS_TERMS.contains(queryParams.q))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(
                    BadRequestDataResponse(
                        "The parameter q only accepts as a value one or more of $ALL_IAM_RIGHTS_TERMS"
                    )
                )

        val countAndAuthorizedEntities = authorizationService.getAuthorizedEntities(
            queryParams,
            sub,
            contextLink
        )

        if (countAndAuthorizedEntities.first == -1) {
            return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = JsonLdUtils.compactEntities(
            countAndAuthorizedEntities.second,
            queryParams.useSimplifiedRepresentation,
            contextLink,
            mediaType
        )

        return buildQueryResponse(
            compactedEntities,
            countAndAuthorizedEntities.first,
            "/ngsi-ld/v1/entityAccessControl/entities",
            queryParams,
            params,
            mediaType,
            contextLink
        )
    }

    @GetMapping("/groups", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getGroupsMemberships(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()
        val queryParams = parseAndCheckParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        )

        val countAndGroupEntities =
            authorizationService.getGroupsMemberships(sub, queryParams.offset, queryParams.limit, contextLink)

        if (countAndGroupEntities.first == -1) {
            return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = JsonLdUtils.compactEntities(
            countAndGroupEntities.second,
            queryParams.useSimplifiedRepresentation,
            contextLink,
            mediaType
        )

        return buildQueryResponse(
            compactedEntities,
            countAndGroupEntities.first,
            "/ngsi-ld/v1/entityAccessControl/entities",
            queryParams,
            params,
            mediaType,
            contextLink
        )
    }

    @PostMapping("/{subjectId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addRightsOnEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)

        // ensure payload contains only relationships and that they are of a known type
        val (validAttributes, invalidAttributes) = ngsiLdAttributes.partition {
            it is NgsiLdRelationship &&
                ALL_IAM_RIGHTS.contains(it.name)
        }
        val invalidAttributesDetails = invalidAttributes.map {
            NotUpdatedDetails(it.name, "Not a relationship or not an authorized relationship name")
        }

        val (authorizedInstances, unauthorizedInstances) = validAttributes
            .map { it as NgsiLdRelationship }
            .map { ngsiLdAttribute -> ngsiLdAttribute.getAttributeInstances().map { Pair(ngsiLdAttribute, it) } }
            .flatten()
            .partition {
                // we don't have any sub-relationships here, so let's just take the first
                val targetEntityId = it.second.getLinkedEntitiesIds().first()
                authorizationService.userIsAdminOfEntity(targetEntityId, sub)
            }
        val unauthorizedInstancesDetails = unauthorizedInstances.map {
            NotUpdatedDetails(
                it.first.compactName,
                "User is not authorized to manage rights on entity ${it.second.objectId}"
            )
        }

        val results = authorizedInstances.map {
            entityService.appendEntityRelationship(
                subjectId.toUri(),
                it.first,
                it.second,
                false
            )
        }
        val appendResult = updateResultFromDetailedResult(results)

        if (appendResult.updated.isNotEmpty())
            entityEventService.publishAttributeAppendEvents(
                sub.orNull(),
                subjectId.toUri(),
                jsonLdAttributes,
                appendResult,
                contexts
            )

        return if (invalidAttributes.isEmpty() && unauthorizedInstances.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else {
            val fullAppendResult = appendResult.copy(
                notUpdated = appendResult.notUpdated.plus(invalidAttributesDetails).plus(unauthorizedInstancesDetails)
            )
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(fullAppendResult)
        }
    }

    @DeleteMapping("/{subjectId}/attrs/{entityId}")
    suspend fun removeRightsOnEntity(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders))

        if (!authorizationService.userIsAdminOfEntity(entityId.toUri(), sub))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to manage rights on entity $entityId")

        val removeResult =
            authorizationService.removeUserRightsOnEntity(entityId.toUri(), subjectId.toUri())
                .also {
                    if (it != 0)
                        entityEventService.publishAttributeDeleteEvent(
                            sub = sub.orNull(),
                            entityId = subjectId.toUri(),
                            attributeName = entityId,
                            deleteAll = false,
                            contexts = contexts
                        )
                }

        return if (removeResult != 0)
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            throw ResourceNotFoundException("No right found for $subjectId on $entityId")
    }

    @PostMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun updateSpecificAccessPolicy(
        @PathVariable entityId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val entityUri = entityId.toUri()
        if (!authorizationService.userIsAdminOfEntity(entityUri, sub))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to update access policy on entity $entityId")

        val body = requestBody.awaitFirst()
        val expandedPayload = expandJsonLdFragment(AUTH_TERM_SAP, body, COMPOUND_AUTHZ_CONTEXT)
        val ngsiLdAttributes = parseToNgsiLdAttributes(expandedPayload)
        return when (val checkResult = checkSpecificAccessPolicyPayload(ngsiLdAttributes)) {
            is Invalid -> ResponseEntity.status(HttpStatus.BAD_REQUEST).body(checkResult.value)
            is Valid -> {
                val updateResult = withContext(Dispatchers.IO) {
                    entityService.appendEntityAttributes(entityUri, ngsiLdAttributes, false)
                }

                if (updateResult.updated.isNotEmpty()) {
                    entityEventService.publishAttributeAppendEvent(
                        sub.orNull(),
                        entityUri,
                        AUTH_TERM_SAP,
                        null,
                        true,
                        body,
                        updateResult.updated[0].updateOperationResult,
                        COMPOUND_AUTHZ_CONTEXT
                    )

                    ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
                } else {
                    ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(
                            InternalErrorResponse(
                                "An error occurred while setting policy for $entityId " +
                                    "(${updateResult.notUpdated[0].reason})"
                            )
                        )
                }
            }
        }
    }

    private fun checkSpecificAccessPolicyPayload(ngsiLdAttributes: List<NgsiLdAttribute>): Validated<String, Unit> {
        val ngsiLdAttributeInstances = ngsiLdAttributes[0].getAttributeInstances()
        if (ngsiLdAttributeInstances.size > 1)
            return "Payload must only contain a single attribute instance".invalid()
        val ngsiLdAttributeInstance = ngsiLdAttributeInstances[0]
        if (ngsiLdAttributeInstance !is NgsiLdPropertyInstance)
            return "Payload must be a property".invalid()
        return try {
            AuthContextModel.SpecificAccessPolicy.valueOf(ngsiLdAttributeInstance.value.toString())
            Unit.valid()
        } catch (e: java.lang.IllegalArgumentException) {
            "Value must be one of AUTH_READ or AUTH_WRITE (${e.message})".invalid()
        }
    }

    @DeleteMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun deleteSpecificAccessPolicy(
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val entityUri = entityId.toUri()
        if (!authorizationService.userIsAdminOfEntity(entityUri, sub))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to remove access policy from entity $entityId")

        val deleteResult = withContext(Dispatchers.IO) {
            entityService.deleteEntityAttribute(entityUri, AUTH_PROP_SAP)
        }

        if (deleteResult) {
            entityEventService.publishAttributeDeleteEvent(
                sub.orNull(),
                entityUri,
                AUTH_TERM_SAP,
                null,
                false,
                COMPOUND_AUTHZ_CONTEXT
            )
        }

        return if (deleteResult)
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .contentType(MediaType.APPLICATION_JSON)
                .body(InternalErrorResponse("An error occurred while removing policy from $entityId"))
    }

    @PostMapping("/sync")
    suspend fun syncIam(): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        if (!authorizationService.userIsAdmin(sub))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to sync user referential")

        listOf(AuthContextModel.USER_TYPE, AuthContextModel.GROUP_TYPE, AuthContextModel.CLIENT_TYPE)
            .asSequence()
            .map {
                // do a first search without asking for a result in order to get the total count
                val total = entityService.searchEntities(
                    QueryParams(types = setOf(it), offset = 0, limit = 0),
                    sub,
                    NGSILD_AUTHORIZATION_CONTEXT
                ).first
                logger.debug("Counted a total of $total entities for type $it")
                entityService.searchEntities(
                    QueryParams(types = setOf(it), offset = 0, limit = total),
                    sub,
                    NGSILD_AUTHORIZATION_CONTEXT
                )
            }
            .map { it.second }
            .flatten()
            .forEach { jsonLdEntity ->
                logger.debug("Preparing events for subject: ${jsonLdEntity.id} (${jsonLdEntity.types}")
                // generate an attribute append event per rCanXXX relationship
                val entitiesRightsEvents =
                    listOf(AUTH_REL_CAN_ADMIN, AUTH_REL_CAN_WRITE, AUTH_REL_CAN_READ)
                        .map {
                            generateAttributeAppendEvents(sub.orNull(), jsonLdEntity, it)
                        }
                        .flatten()

                // remove the rCanXXX relationships as they are sent separately
                val updatedEntity = JsonLdUtils.compactAndSerialize(
                    jsonLdEntity.copy(
                        properties = jsonLdEntity.properties.minus(
                            listOf(AUTH_REL_CAN_ADMIN, AUTH_REL_CAN_WRITE, AUTH_REL_CAN_READ)
                        ),
                    ),
                    COMPOUND_AUTHZ_CONTEXT,
                    MediaType.APPLICATION_JSON
                )
                val iamEvent = EntityCreateEvent(
                    sub.orNull(),
                    jsonLdEntity.id.toUri(),
                    compactTerms(jsonLdEntity.types, COMPOUND_AUTHZ_CONTEXT),
                    updatedEntity,
                    COMPOUND_AUTHZ_CONTEXT
                )
                kafkaTemplate.send("cim.iam.replay", iamEvent.entityId.toString(), serializeObject(iamEvent))

                entitiesRightsEvents.forEach {
                    kafkaTemplate.send("cim.iam.replay", it.entityId.toString(), serializeObject(it))
                }
                logger.debug("Sent events for subject ${jsonLdEntity.id}")
            }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }

    private fun generateAttributeAppendEvents(
        sub: String?,
        jsonLdEntity: JsonLdEntity,
        attribute: ExpandedTerm
    ): List<AttributeAppendEvent> =
        if (jsonLdEntity.properties.containsKey(attribute)) {
            when (val rightRel = jsonLdEntity.properties[attribute]) {
                is Map<*, *> ->
                    listOf(
                        attributeToAppendEvent(sub, jsonLdEntity, rightRel, attribute)
                    )
                is List<*> ->
                    rightRel.map { rightRelInstance ->
                        attributeToAppendEvent(
                            sub,
                            jsonLdEntity,
                            rightRelInstance as Map<*, *>,
                            attribute
                        )
                    }
                else -> {
                    logger.warn("Unsupported representation for $attribute: $rightRel")
                    emptyList()
                }
            }
        } else emptyList()

    private fun attributeToAppendEvent(
        sub: String?,
        jsonLdEntity: JsonLdEntity,
        attributePayload: Map<*, *>,
        attribute: ExpandedTerm,
    ): AttributeAppendEvent {
        val datasetId =
            if (attributePayload.containsKey(NGSILD_DATASET_ID_PROPERTY))
                ((attributePayload[NGSILD_DATASET_ID_PROPERTY] as Map<String, Any>)[JSONLD_ID] as String).toUri()
            else null
        return AttributeAppendEvent(
            sub,
            jsonLdEntity.id.toUri(),
            compactTerms(jsonLdEntity.types, COMPOUND_AUTHZ_CONTEXT),
            attribute.toCompactTerm(),
            datasetId,
            true,
            serializeObject(JsonLdUtils.compactFragment(attributePayload as Map<String, Any>, COMPOUND_AUTHZ_CONTEXT)),
            "",
            COMPOUND_AUTHZ_CONTEXT
        )
    }

    @PostMapping("/syncSap")
    suspend fun syncSap(): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        if (!authorizationService.userIsAdmin(sub))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to sync user referential")
        val total = entityService.searchEntities(
            QueryParams(attrs = setOf(AUTH_PROP_SAP), offset = 0, limit = 0),
            sub,
            NGSILD_AUTHORIZATION_CONTEXT
        ).first
        logger.debug("Counted a total of $total entities for attribute $AUTH_PROP_SAP")
        entityService.searchEntities(
            QueryParams(attrs = setOf(AUTH_PROP_SAP), offset = 0, limit = total),
            sub,
            NGSILD_AUTHORIZATION_CONTEXT
        ).second
            .forEach { jsonLdEntity ->
                val event = attributeToAppendEvent(
                    sub.orNull(),
                    jsonLdEntity,
                    (jsonLdEntity.properties[AUTH_PROP_SAP] as List<*>)[0] as Map<*, *>,
                    AUTH_PROP_SAP
                )
                kafkaTemplate.send("cim.iam.rights", jsonLdEntity.id, serializeObject(event))
                logger.debug("Sent event for entity: ${jsonLdEntity.id}")
            }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }
}
