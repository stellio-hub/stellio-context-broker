package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.NGSILD_EGM_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.compactFragment
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.extractSubjectOrEmpty
import com.egm.stellio.shared.util.toCompactTerm
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/entity/admin")
class IAMSynchronizer(
    private val entityService: EntityService,
    private val authorizationService: AuthorizationService,
    private val kafkaTemplate: KafkaTemplate<String, String>,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @PostMapping("/iam/sync")
    suspend fun syncIam(): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        if (!authorizationService.userIsAdmin(userId))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to sync user referential")

        val authorizationContexts = listOf(NGSILD_EGM_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT)
        listOf(USER_TYPE, GROUP_TYPE, CLIENT_TYPE)
            .asSequence()
            .map {
                // do a first search without asking for a result in order to get the total count
                val total = entityService.searchEntities(
                    QueryParams(expandedType = it),
                    userId,
                    0,
                    0,
                    NGSILD_EGM_AUTHORIZATION_CONTEXT,
                    false
                ).first
                entityService.searchEntities(
                    QueryParams(expandedType = it),
                    userId,
                    0,
                    total,
                    NGSILD_EGM_AUTHORIZATION_CONTEXT,
                    false
                )
            }
            .map { it.second }
            .flatten()
            .map { jsonLdEntity ->
                // generate an attribute append event per rCanXXX relationship
                val entitiesRightsEvents =
                    generateAttributeAppendEvents(jsonLdEntity, AUTH_REL_CAN_ADMIN, authorizationContexts)
                        .plus(generateAttributeAppendEvents(jsonLdEntity, AUTH_REL_CAN_WRITE, authorizationContexts))
                        .plus(generateAttributeAppendEvents(jsonLdEntity, AUTH_REL_CAN_READ, authorizationContexts))

                // remove the rCanXXX relationships as they are sent separately
                val updatedEntity = compactAndSerialize(
                    jsonLdEntity.copy(
                        properties = jsonLdEntity.properties.minus(
                            listOf(AUTH_REL_CAN_ADMIN, AUTH_REL_CAN_WRITE, AUTH_REL_CAN_READ)
                        ),
                    ),
                    authorizationContexts,
                    MediaType.APPLICATION_JSON
                )
                val iamEvent = EntityCreateEvent(
                    jsonLdEntity.id.toUri(),
                    jsonLdEntity.type.toCompactTerm(),
                    updatedEntity,
                    authorizationContexts
                )
                listOf(iamEvent).plus(entitiesRightsEvents)
            }
            .flatten()
            .toList()
            .forEach {
                val serializedEvent = serializeObject(it)
                logger.debug("Sending event: $serializedEvent")
                kafkaTemplate.send("cim.iam.replay", it.entityId.toString(), serializedEvent)
            }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }

    private fun generateAttributeAppendEvents(
        jsonLdEntity: JsonLdEntity,
        accessRight: ExpandedTerm,
        authorizationContexts: List<String>
    ): List<AttributeAppendEvent> =
        if (jsonLdEntity.properties.containsKey(accessRight)) {
            when (val rightRel = jsonLdEntity.properties[accessRight]) {
                is Map<*, *> ->
                    listOf(rightRelToAttributeAppendEvent(jsonLdEntity, rightRel, accessRight, authorizationContexts))
                is List<*> ->
                    rightRel.map { rightRelInstance ->
                        rightRelToAttributeAppendEvent(
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
        jsonLdEntity: JsonLdEntity,
        rightRel: Map<*, *>,
        accessRight: ExpandedTerm,
        authorizationContexts: List<String>
    ): AttributeAppendEvent =
        AttributeAppendEvent(
            jsonLdEntity.id.toUri(),
            jsonLdEntity.type.toCompactTerm(),
            accessRight.toCompactTerm(),
            ((rightRel[NGSILD_DATASET_ID_PROPERTY] as Map<String, Any>)[JSONLD_ID] as String).toUri(),
            true,
            serializeObject(compactFragment(rightRel as Map<String, Any>, authorizationContexts)),
            "",
            authorizationContexts
        )
}
