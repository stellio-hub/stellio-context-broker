package com.egm.stellio.search.service

import arrow.core.None
import arrow.core.Some
import arrow.core.flattenOption
import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SID
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.net.URI

@Component
class IAMListener(
    private val subjectReferentialService: SubjectReferentialService,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService
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
            is AttributeAppendEvent -> handleAttributeAppendOnRights(authorizationEvent)
            is AttributeReplaceEvent -> handleAttributeReplaceOnRights(authorizationEvent)
            is AttributeDeleteEvent -> handleAttributeDeleteOnRights(authorizationEvent)
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    @KafkaListener(topics = ["cim.iam.replay"], groupId = "search-iam-replay")
    fun processIamReplay(content: String) {
        logger.debug("Received IAM replay event: $content")
        when (val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> createFullSubjectReferential(authorizationEvent)
            is AttributeAppendEvent ->
                addEntityToSubject(
                    authorizationEvent.operationPayload,
                    authorizationEvent.attributeName,
                    authorizationEvent.entityId
                )
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    private fun createFullSubjectReferential(entityCreateEvent: EntityCreateEvent) {
        val operationPayloadNode = mapper.readTree(entityCreateEvent.operationPayload)
        val roles = extractRoles(operationPayloadNode)
        val serviceAccountId =
            if (operationPayloadNode.has(AUTH_TERM_SID))
                (operationPayloadNode[AUTH_TERM_SID] as ObjectNode)[JSONLD_VALUE].asText()
            else null
        val groupsMemberships = extractGroupsMemberships(operationPayloadNode)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSub(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityTypes.first().uppercase()),
            globalRoles = roles,
            serviceAccountId = serviceAccountId?.extractSub(),
            groupsMemberships = groupsMemberships
        )

        subjectReferentialService.create(subjectReferential).subscribe()
    }

    private fun createSubjectReferential(entityCreateEvent: EntityCreateEvent) {
        val operationPayloadNode = mapper.readTree(entityCreateEvent.operationPayload)
        val roles = extractRoles(operationPayloadNode)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSub(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityTypes.first().uppercase()),
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
        val operationPayloadNode = mapper.readTree(attributeAppendEvent.operationPayload)
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

    private fun handleAttributeReplaceOnRights(attributeReplaceEvent: AttributeReplaceEvent) {
        if (attributeReplaceEvent.attributeName == AUTH_TERM_SAP) {
            updateSpecificAccessPolicy(attributeReplaceEvent.operationPayload, attributeReplaceEvent.entityId)
        } else {
            addEntityToSubject(
                attributeReplaceEvent.operationPayload,
                attributeReplaceEvent.attributeName,
                attributeReplaceEvent.entityId
            )
        }
    }

    private fun handleAttributeAppendOnRights(attributeAppendEvent: AttributeAppendEvent) {
        if (attributeAppendEvent.attributeName == AUTH_TERM_SAP) {
            updateSpecificAccessPolicy(attributeAppendEvent.operationPayload, attributeAppendEvent.entityId)
        } else {
            addEntityToSubject(
                attributeAppendEvent.operationPayload,
                attributeAppendEvent.attributeName,
                attributeAppendEvent.entityId
            )
        }
    }

    private fun updateSpecificAccessPolicy(operationPayload: String, entityId: URI) {
        val operationPayloadNode = mapper.readTree(operationPayload)
        val policy = SpecificAccessPolicy.valueOf(operationPayloadNode["value"].asText())
        temporalEntityAttributeService.updateSpecificAccessPolicy(entityId, policy)
            .subscribe {
                logger.debug("Updated specific access policy for entity $entityId ($it records updated)")
            }
    }

    private fun addEntityToSubject(operationPayload: String, attributeName: String, subjectId: URI) {
        val operationPayloadNode = mapper.readTree(operationPayload)
        val entityId = operationPayloadNode["object"].asText()
        when (val accessRight = AccessRight.forAttributeName(attributeName)) {
            is Some ->
                entityAccessRightsService.setRoleOnEntity(
                    subjectId.extractSub(),
                    entityId.toUri(),
                    accessRight.value
                ).subscribe()
            is None -> logger.warn("Unable to extract a known access right from $attributeName")
        }
    }

    private fun handleAttributeDeleteOnRights(attributeDeleteEvent: AttributeDeleteEvent) {
        if (attributeDeleteEvent.attributeName == AUTH_TERM_SAP) {
            removeSpecificAccessPolicy(attributeDeleteEvent)
        } else {
            removeEntityFromSubject(attributeDeleteEvent)
        }
    }

    private fun removeSpecificAccessPolicy(attributeDeleteEvent: AttributeDeleteEvent) {
        val entityId = attributeDeleteEvent.entityId
        temporalEntityAttributeService.removeSpecificAccessPolicy(entityId)
            .subscribe {
                logger.debug("Removed specific access policy for entity $entityId ($it records updated)")
            }
    }

    private fun removeEntityFromSubject(attributeDeleteEvent: AttributeDeleteEvent) {
        entityAccessRightsService.removeRoleOnEntity(
            attributeDeleteEvent.entityId.extractSub(),
            attributeDeleteEvent.attributeName.toUri()
        ).subscribe()
    }
}
