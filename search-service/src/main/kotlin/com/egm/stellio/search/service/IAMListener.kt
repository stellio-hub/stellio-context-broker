package com.egm.stellio.search.service

import arrow.core.None
import arrow.core.Some
import arrow.core.flattenOption
import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SID
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.extractSub
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
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
    fun processIam(content: String) {
        logger.debug("Received IAM event: $content")
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
        logger.debug("Received IAM rights event: $content")
        when (val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)) {
            is AttributeAppendEvent -> addEntityToSubject(authorizationEvent)
            is AttributeDeleteEvent -> removeEntityFromSubject(authorizationEvent)
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    @KafkaListener(topics = ["cim.iam.replay"], groupId = "search-iam-replay")
    fun processIamReplay(content: String) {
        logger.debug("Received IAM replay event: $content")
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
            if (operationPayloadNode.has(AUTH_TERM_SID))
                (operationPayloadNode[AUTH_TERM_SID] as ObjectNode)[JSONLD_VALUE].asText()
            else null
        val groupsMemberships = extractGroupsMemberships(operationPayloadNode)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSub(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityType.uppercase()),
            globalRoles = roles,
            serviceAccountId = serviceAccountId?.extractSub(),
            groupsMemberships = groupsMemberships
        )

        subjectReferentialService.create(subjectReferential).subscribe()
    }

    private fun createSubjectReferential(entityCreateEvent: EntityCreateEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(entityCreateEvent.operationPayload)
        val roles = extractRoles(operationPayloadNode)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSub(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityType.uppercase()),
            globalRoles = roles
        )

        subjectReferentialService.create(subjectReferential).subscribe()
    }

    private fun extractGroupsMemberships(operationPayloadNode: JsonNode): List<Sub>? =
        if (operationPayloadNode.has(AUTH_TERM_IS_MEMBER_OF)) {
            when (val isMemberOf = operationPayloadNode[AUTH_TERM_IS_MEMBER_OF]) {
                is ObjectNode -> listOf(isMemberOf["object"].asText().extractSub())
                is ArrayNode ->
                    isMemberOf.map {
                        it["object"].asText().extractSub()
                    }.ifEmpty { null }
                else -> null
            }
        } else null

    private fun extractRoles(operationPayloadNode: JsonNode): List<GlobalRole>? =
        if (operationPayloadNode.has(AUTH_TERM_ROLES)) {
            when (val rolesValue = (operationPayloadNode[AUTH_TERM_ROLES] as ObjectNode)[JSONLD_VALUE]) {
                is TextNode -> GlobalRole.forKey(rolesValue.asText()).map { listOf(it) }.orNull()
                is ArrayNode ->
                    rolesValue.map {
                        GlobalRole.forKey(it.asText())
                    }.flattenOption()
                else -> null
            }
        } else null

    private fun deleteSubjectReferential(entityDeleteEvent: EntityDeleteEvent) {
        subjectReferentialService.delete(entityDeleteEvent.entityId.extractSub())
            .subscribe {
                logger.debug("Deleted subject ${entityDeleteEvent.entityId}")
            }
    }

    private fun updateSubjectProfile(attributeAppendEvent: AttributeAppendEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
        val subjectUuid = attributeAppendEvent.entityId.extractSub()
        if (attributeAppendEvent.attributeName == AUTH_TERM_ROLES) {
            val newRoles = (operationPayloadNode[JSONLD_VALUE] as ArrayNode).map {
                GlobalRole.forKey(it.asText())
            }.flattenOption()
            if (newRoles.isNotEmpty())
                subjectReferentialService.setGlobalRoles(subjectUuid, newRoles).subscribe()
            else
                subjectReferentialService.resetGlobalRoles(subjectUuid).subscribe()
        } else if (attributeAppendEvent.attributeName == AUTH_TERM_SID) {
            val serviceAccountId = operationPayloadNode[JSONLD_VALUE].asText()
            subjectReferentialService.addServiceAccountIdToClient(
                subjectUuid,
                serviceAccountId.extractSub()
            ).subscribe()
        } else if (attributeAppendEvent.attributeName == AUTH_TERM_IS_MEMBER_OF) {
            val groupId = operationPayloadNode["object"].asText()
            subjectReferentialService.addGroupMembershipToUser(
                subjectUuid,
                groupId.extractSub()
            ).subscribe()
        } else {
            logger.info("Received unknown attribute name: ${attributeAppendEvent.attributeName}")
        }
    }

    private fun removeSubjectFromGroup(attributeDeleteEvent: AttributeDeleteEvent) {
        subjectReferentialService.removeGroupMembershipToUser(
            attributeDeleteEvent.entityId.extractSub(),
            attributeDeleteEvent.datasetId!!.extractSub()
        ).subscribe()
    }

    private fun addEntityToSubject(attributeAppendEvent: AttributeAppendEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
        val entityId = operationPayloadNode["object"].asText()
        when (val accessRight = AccessRight.forAttributeName(attributeAppendEvent.attributeName)) {
            is Some ->
                entityAccessRightsService.setRoleOnEntity(
                    attributeAppendEvent.entityId.extractSub(),
                    entityId.toUri(),
                    accessRight.value
                ).subscribe()
            is None -> logger.warn("Unable to extract a known access right from $accessRight")
        }
    }

    private fun removeEntityFromSubject(attributeDeleteEvent: AttributeDeleteEvent) {
        entityAccessRightsService.removeRoleOnEntity(
            attributeDeleteEvent.entityId.extractSub(),
            attributeDeleteEvent.attributeName.toUri()
        ).subscribe()
    }
}
