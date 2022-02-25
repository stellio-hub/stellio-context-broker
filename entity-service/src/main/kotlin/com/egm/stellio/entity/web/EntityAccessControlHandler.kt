package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.updateResultFromDetailedResult
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.NGSILD_EGM_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
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
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/entityAccessControl")
class EntityAccessControlHandler(
    private val entityService: EntityService,
    private val authorizationService: AuthorizationService,
    private val entityEventService: EntityEventService,
    private val kafkaTemplate: KafkaTemplate<String, String>,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @PostMapping("/{subjectId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addRightsOnEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = JsonLdUtils.expandJsonLdFragment(body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)

        // ensure payload contains only relationships and that they are of a known type
        val (validAttributes, invalidAttributes) = ngsiLdAttributes.partition {
            it is NgsiLdRelationship &&
                ALL_IAM_RIGHTS.contains(it.name)
        }
        val invalidAttributesDetails = invalidAttributes.map {
            NotUpdatedDetails(it.compactName, "Not a relationship or not an authorized relationship name")
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
        if (!authorizationService.userIsAdminOfEntity(entityId.toUri(), sub))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to update access policy on entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = listOf(NGSILD_EGM_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT)
        val expandedPayload = JsonLdUtils.parseAndExpandAttributeFragment(AUTH_TERM_SAP, body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(expandedPayload)
        val updateResult = withContext(Dispatchers.IO) {
            entityService.appendEntityAttributes(entityId.toUri(), ngsiLdAttributes, false)
        }

        if (updateResult.updated.isNotEmpty()) {
            entityEventService.publishAttributeAppendEvent(
                sub.orNull(),
                entityId.toUri(),
                AUTH_TERM_SAP,
                null,
                true,
                body,
                updateResult.updated[0].updateOperationResult,
                contexts
            )
        }

        return if (updateResult.notUpdated.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(updateResult)
    }

    @DeleteMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun deleteSpecificAccessPolicy(
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        if (!authorizationService.userIsAdminOfEntity(entityId.toUri(), sub))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to remove access policy from entity $entityId")

        val contexts = listOf(NGSILD_EGM_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT)
        val deleteResult = withContext(Dispatchers.IO) {
            entityService.deleteEntityAttribute(entityId.toUri(), AUTH_PROP_SAP)
        }

        if (deleteResult) {
            entityEventService.publishAttributeDeleteEvent(
                sub.orNull(),
                entityId.toUri(),
                AUTH_TERM_SAP,
                null,
                true,
                contexts
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

        val authorizationContexts = listOf(
            NGSILD_EGM_AUTHORIZATION_CONTEXT,
            JsonLdUtils.NGSILD_CORE_CONTEXT
        )
        listOf(AuthContextModel.USER_TYPE, AuthContextModel.GROUP_TYPE, AuthContextModel.CLIENT_TYPE)
            .asSequence()
            .map {
                // do a first search without asking for a result in order to get the total count
                val total = entityService.searchEntities(
                    QueryParams(expandedType = it),
                    sub,
                    0,
                    0,
                    NGSILD_EGM_AUTHORIZATION_CONTEXT,
                    false
                ).first
                logger.debug("Counted a total of $total entities for type $it")
                entityService.searchEntities(
                    QueryParams(expandedType = it),
                    sub,
                    0,
                    total,
                    NGSILD_EGM_AUTHORIZATION_CONTEXT,
                    false
                )
            }
            .map { it.second }
            .flatten()
            .forEach { jsonLdEntity ->
                logger.debug("Preparing events for subject: ${jsonLdEntity.id} (${jsonLdEntity.type}")
                // generate an attribute append event per rCanXXX relationship
                val entitiesRightsEvents =
                    listOf(AUTH_REL_CAN_ADMIN, AUTH_REL_CAN_WRITE, AUTH_REL_CAN_READ)
                        .map {
                            generateAttributeAppendEvents(sub.orNull(), jsonLdEntity, it, authorizationContexts)
                        }
                        .flatten()

                // remove the rCanXXX relationships as they are sent separately
                val updatedEntity = JsonLdUtils.compactAndSerialize(
                    jsonLdEntity.copy(
                        properties = jsonLdEntity.properties.minus(
                            listOf(AUTH_REL_CAN_ADMIN, AUTH_REL_CAN_WRITE, AUTH_REL_CAN_READ)
                        ),
                    ),
                    authorizationContexts,
                    MediaType.APPLICATION_JSON
                )
                val iamEvent = EntityCreateEvent(
                    sub.orNull(),
                    jsonLdEntity.id.toUri(),
                    jsonLdEntity.type.toCompactTerm(),
                    updatedEntity,
                    authorizationContexts
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
        accessRight: ExpandedTerm,
        authorizationContexts: List<String>
    ): List<AttributeAppendEvent> =
        if (jsonLdEntity.properties.containsKey(accessRight)) {
            when (val rightRel = jsonLdEntity.properties[accessRight]) {
                is Map<*, *> ->
                    listOf(
                        rightRelToAttributeAppendEvent(sub, jsonLdEntity, rightRel, accessRight, authorizationContexts)
                    )
                is List<*> ->
                    rightRel.map { rightRelInstance ->
                        rightRelToAttributeAppendEvent(
                            sub,
                            jsonLdEntity,
                            rightRelInstance as Map<*, *>,
                            accessRight,
                            authorizationContexts
                        )
                    }
                else -> {
                    logger.warn("Unsupported representation for $accessRight: $rightRel")
                    emptyList()
                }
            }
        } else emptyList()

    private fun rightRelToAttributeAppendEvent(
        sub: String?,
        jsonLdEntity: JsonLdEntity,
        rightRel: Map<*, *>,
        accessRight: ExpandedTerm,
        authorizationContexts: List<String>
    ): AttributeAppendEvent =
        AttributeAppendEvent(
            sub,
            jsonLdEntity.id.toUri(),
            jsonLdEntity.type.toCompactTerm(),
            accessRight.toCompactTerm(),
            ((rightRel[NGSILD_DATASET_ID_PROPERTY] as Map<String, Any>)[JSONLD_ID] as String).toUri(),
            true,
            serializeObject(JsonLdUtils.compactFragment(rightRel as Map<String, Any>, authorizationContexts)),
            "",
            authorizationContexts
        )
}
