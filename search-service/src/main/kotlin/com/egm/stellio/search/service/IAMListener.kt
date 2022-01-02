package com.egm.stellio.search.service

import arrow.core.flattenOption
import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.extractSubjectUuid
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class IAMListener(
    private val subjectReferentialService: SubjectReferentialService,
    private val entityAccessRightsService: EntityAccessRightsService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["cim.iam"], groupId = "search-iam")
    fun processIam(content: String) {
        logger.debug("Received event: $content")
        when (val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> createSubjectReferential(authorizationEvent)
            is EntityDeleteEvent -> deleteSubjectReferential(authorizationEvent)
            is AttributeAppendEvent -> updateSubjectProfile(authorizationEvent)
            is AttributeReplaceEvent -> logger.debug("Not interested in attribute replace events for IAM events")
            is AttributeDeleteEvent -> removeSubjectFromGroup(authorizationEvent)
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    @KafkaListener(topics = ["cim.iam.rights"], groupId = "search-iam-rights")
    fun processIamRights(content: String) {
        when (val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)) {
            is AttributeAppendEvent -> addEntityToSubject(authorizationEvent)
            is AttributeDeleteEvent -> removeEntityFromSubject(authorizationEvent)
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    @KafkaListener(topics = ["cim.iam.replay"], groupId = "search-iam-replay")
    fun processIamReplay(content: String) {
        logger.debug("Received event: $content")
        when (val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> createFullSubjectReferential(authorizationEvent)
            is AttributeAppendEvent -> addEntityToSubject(authorizationEvent)
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    private fun createFullSubjectReferential(entityCreateEvent: EntityCreateEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(entityCreateEvent.operationPayload)
        val roles = extractRoles(operationPayloadNode)
        val serviceAccountId =
            if (operationPayloadNode.has("serviceAccountId"))
                (operationPayloadNode["serviceAccountId"] as ObjectNode)["value"].asText()
            else null
        val groupsMemberships = extractGroupsMemberships(operationPayloadNode)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSubjectUuid(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityType.uppercase()),
            globalRoles = roles,
            serviceAccountId = serviceAccountId?.extractSubjectUuid(),
            groupsMemberships = groupsMemberships
        )

        subjectReferentialService.create(subjectReferential)
            .subscribe {
                logger.debug("Created subject ${entityCreateEvent.entityId}")
            }
    }

    private fun createSubjectReferential(entityCreateEvent: EntityCreateEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(entityCreateEvent.operationPayload)
        val roles = extractRoles(operationPayloadNode)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSubjectUuid(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityType.uppercase()),
            globalRoles = roles
        )

        subjectReferentialService.create(subjectReferential)
            .subscribe {
                logger.debug("Created subject ${entityCreateEvent.entityId}")
            }
    }

    private fun extractGroupsMemberships(operationPayloadNode: JsonNode): List<UUID>? =
        if (operationPayloadNode.has("isMemberOf")) {
            when (val isMemberOf = operationPayloadNode["isMemberOf"]) {
                is ObjectNode -> listOf(isMemberOf["object"].asText().extractSubjectUuid())
                is ArrayNode ->
                    isMemberOf.map {
                        it["object"].asText().extractSubjectUuid()
                    }.ifEmpty { null }
                else -> null
            }
        } else null

    private fun extractRoles(operationPayloadNode: JsonNode): List<GlobalRole>? =
        if (operationPayloadNode.has("roles")) {
            when (val rolesValue = (operationPayloadNode["roles"] as ObjectNode)["value"]) {
                is TextNode -> GlobalRole.forKey(rolesValue.asText()).map { listOf(it) }.orNull()
                is ArrayNode ->
                    rolesValue.map {
                        GlobalRole.forKey(it.asText())
                    }.flattenOption()
                else -> null
            }
        } else null

    private fun deleteSubjectReferential(entityDeleteEvent: EntityDeleteEvent) {
        subjectReferentialService.delete(entityDeleteEvent.entityId.extractSubjectUuid())
            .subscribe {
                logger.debug("Deleted subject ${entityDeleteEvent.entityId}")
            }
    }

    private fun updateSubjectProfile(attributeAppendEvent: AttributeAppendEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
        val subjectUuid = attributeAppendEvent.entityId.extractSubjectUuid()
        if (attributeAppendEvent.attributeName == "roles") {
            val newRoles = (operationPayloadNode["value"] as ArrayNode).map {
                GlobalRole.forKey(it.asText())
            }.flattenOption()
            if (newRoles.isNotEmpty())
                subjectReferentialService.setGlobalRoles(subjectUuid, newRoles).subscribe()
            else
                subjectReferentialService.resetGlobalRoles(subjectUuid).subscribe()
        } else if (attributeAppendEvent.attributeName == "serviceAccountId") {
            val serviceAccountId = operationPayloadNode["value"].asText()
            subjectReferentialService.addServiceAccountIdToClient(
                subjectUuid,
                serviceAccountId.extractSubjectUuid()
            ).subscribe()
        } else if (attributeAppendEvent.attributeName == "isMemberOf") {
            val groupId = operationPayloadNode["object"].asText()
            subjectReferentialService.addGroupMembershipToUser(
                subjectUuid,
                groupId.extractSubjectUuid()
            ).subscribe()
        } else {
            logger.info("Received unknown attribute name: ${attributeAppendEvent.attributeName}")
        }
    }

    private fun removeSubjectFromGroup(attributeDeleteEvent: AttributeDeleteEvent) {
        subjectReferentialService.removeGroupMembershipToUser(
            attributeDeleteEvent.entityId.extractSubjectUuid(),
            attributeDeleteEvent.datasetId!!.extractSubjectUuid()
        ).subscribe()
    }

    private fun addEntityToSubject(attributeAppendEvent: AttributeAppendEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
        val entityId = operationPayloadNode["object"].asText()
        entityAccessRightsService.setRoleOnEntity(
            attributeAppendEvent.entityId.extractSubjectUuid(),
            entityId.toUri(),
            AccessRight.forAttributeName(attributeAppendEvent.attributeName)
        ).subscribe()
    }

    private fun removeEntityFromSubject(attributeDeleteEvent: AttributeDeleteEvent) {
        entityAccessRightsService.removeRoleOnEntity(
            attributeDeleteEvent.entityId.extractSubjectUuid(),
            attributeDeleteEvent.attributeName.toUri()
        ).subscribe()
    }
}
