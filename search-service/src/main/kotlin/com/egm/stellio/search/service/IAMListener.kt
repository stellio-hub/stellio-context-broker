package com.egm.stellio.search.service

import com.egm.stellio.search.model.SubjectAccessRights
import com.egm.stellio.search.model.getSubjectTypeFromSubjectId
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.util.JsonUtils
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class IAMListener(
    private val subjectAccessRightsService: SubjectAccessRightsService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["cim.iam"], groupId = "search-iam")
    fun processMessage(content: String) {
        when (val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> createSubjectAccessRights(authorizationEvent)
            is EntityDeleteEvent -> deleteSubjectAccessRights(authorizationEvent)
            is AttributeAppendEvent -> addRoleToSubject(authorizationEvent)
            is AttributeReplaceEvent -> TODO()
            is AttributeDeleteEvent -> TODO()
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    private fun createSubjectAccessRights(entityCreateEvent: EntityCreateEvent) {
        val userAccessRights = SubjectAccessRights(
            subjectId = entityCreateEvent.entityId,
            subjectType = getSubjectTypeFromSubjectId(entityCreateEvent.entityId)
        )

        subjectAccessRightsService.create(userAccessRights)
            .subscribe {
                logger.debug("Created subject ${entityCreateEvent.entityId}")
            }
    }

    private fun deleteSubjectAccessRights(entityDeleteEvent: EntityDeleteEvent) {
        subjectAccessRightsService.delete(entityDeleteEvent.entityId)
            .subscribe {
                logger.debug("Deleted subject ${entityDeleteEvent.entityId}")
            }
    }

    private fun addRoleToSubject(attributeAppendEvent: AttributeAppendEvent) {
        if (attributeAppendEvent.attributeName == "roles") {
            val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
            val updatedRoles = (operationPayloadNode["roles"]["value"] as ArrayNode).elements()
            var hasStellioAdminRole = false
            while (updatedRoles.hasNext()) {
                if (updatedRoles.next().asText().equals("stellio-admin")) {
                    hasStellioAdminRole = true
                    break
                }
            }
            if (hasStellioAdminRole)
                subjectAccessRightsService.addAdminGlobalRole(attributeAppendEvent.entityId)
            else
                subjectAccessRightsService.removeAdminGlobalRole(attributeAppendEvent.entityId)
        }
    }
}
