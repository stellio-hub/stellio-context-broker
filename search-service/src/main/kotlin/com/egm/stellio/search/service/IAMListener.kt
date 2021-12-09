package com.egm.stellio.search.service

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
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class IAMListener(
    private val subjectReferentialService: SubjectReferentialService,
    private val entityAccessRightsService: EntityAccessRightsService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["cim.iam"], groupId = "search-iam")
    fun processMessage(content: String) {
        when (val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> createSubjectReferential(authorizationEvent)
            is EntityDeleteEvent -> deleteSubjectReferential(authorizationEvent)
            is AttributeAppendEvent -> addRoleToSubject(authorizationEvent)
            is AttributeReplaceEvent -> TODO()
            is AttributeDeleteEvent -> TODO()
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

    private fun createSubjectReferential(entityCreateEvent: EntityCreateEvent) {
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSubjectUuid(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityType.uppercase())
        )

        subjectReferentialService.create(subjectReferential)
            .subscribe {
                logger.debug("Created subject ${entityCreateEvent.entityId}")
            }
    }

    private fun deleteSubjectReferential(entityDeleteEvent: EntityDeleteEvent) {
        subjectReferentialService.delete(entityDeleteEvent.entityId.extractSubjectUuid())
            .subscribe {
                logger.debug("Deleted subject ${entityDeleteEvent.entityId}")
            }
    }

    private fun addRoleToSubject(attributeAppendEvent: AttributeAppendEvent) {
        if (attributeAppendEvent.attributeName == "roles") {
            val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
            val updatedRoles = (operationPayloadNode["value"] as ArrayNode).elements()
            val newRoles = updatedRoles.asSequence().map {
                GlobalRole.forKey(it.asText())
            }.toList()
            if (newRoles.isNotEmpty())
                subjectReferentialService.setGlobalRoles(attributeAppendEvent.entityId.extractSubjectUuid(), newRoles)
            else
                subjectReferentialService.resetGlobalRoles(attributeAppendEvent.entityId.extractSubjectUuid())
        }
    }

    private fun addEntityToSubject(attributeAppendEvent: AttributeAppendEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
        val entityId = operationPayloadNode["object"].asText()
        entityAccessRightsService.setRoleOnEntity(
            attributeAppendEvent.entityId.extractSubjectUuid(),
            entityId.toUri(),
            AccessRight.forAttributeName(attributeAppendEvent.attributeName)
        )
    }

    private fun removeEntityFromSubject(attributeDeleteEvent: AttributeDeleteEvent) {
        entityAccessRightsService.removeRoleOnEntity(
            attributeDeleteEvent.entityId.extractSubjectUuid(),
            attributeDeleteEvent.attributeName.toUri()
        )
    }
}
