package com.egm.stellio.search.service

import arrow.core.*
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.authorization.SubjectReferentialService
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
    suspend fun processIam(content: String) {
        logger.debug("Received IAM event: $content")
        val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)
        when (authorizationEvent) {
            is EntityCreateEvent -> createSubjectReferential(authorizationEvent)
            is EntityDeleteEvent -> deleteSubjectReferential(authorizationEvent)
            is AttributeAppendEvent -> updateSubjectProfile(authorizationEvent)
            is AttributeReplaceEvent -> "Not interested in attribute replace events for IAM events".right()
            is AttributeDeleteEvent -> removeSubjectFromGroup(authorizationEvent)
            else -> "Authorization event ${authorizationEvent.operationType} not handled.".right()
        }.fold({
            logger.error(
                "Error while handling event ${authorizationEvent.operationType}" +
                    " for entity ${authorizationEvent.entityId}: $it"
            )
        }, {
            logger.debug(
                "Successfully handled event ${authorizationEvent.operationType}" +
                    " for entity ${authorizationEvent.entityId}"
            )
        })
    }

    @KafkaListener(topics = ["cim.iam.rights"], groupId = "search-iam-rights")
    suspend fun processIamRights(content: String) {
        logger.debug("Received IAM rights event: $content")
        val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)
        when (authorizationEvent) {
            is AttributeAppendEvent -> handleAttributeAppendOnRights(authorizationEvent)
            is AttributeReplaceEvent -> handleAttributeReplaceOnRights(authorizationEvent)
            is AttributeDeleteEvent -> handleAttributeDeleteOnRights(authorizationEvent)
            else -> "Authorization event ${authorizationEvent.operationType} not handled.".right()
        }.fold({
            logger.error(
                "Error while handling event ${authorizationEvent.operationType}" +
                    " for entity ${authorizationEvent.entityId}: $it"
            )
        }, {
            logger.debug(
                "Successfully handled event ${authorizationEvent.operationType}" +
                    " for entity ${authorizationEvent.entityId}"
            )
        })
    }

    @KafkaListener(topics = ["cim.iam.replay"], groupId = "search-iam-replay")
    suspend fun processIamReplay(content: String) {
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

    private suspend fun createFullSubjectReferential(entityCreateEvent: EntityCreateEvent) {
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

        subjectReferentialService.create(subjectReferential)
            .fold({
                logger.warn("Error while creating full subject referential for ${entityCreateEvent.entityId} ($it)")
            }, {
                logger.debug("Created full subject referential for ${entityCreateEvent.entityId}")
            })
    }

    private suspend fun createSubjectReferential(entityCreateEvent: EntityCreateEvent): Either<APIException, Unit> {
        val operationPayloadNode = mapper.readTree(entityCreateEvent.operationPayload)
        val roles = extractRoles(operationPayloadNode)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSub(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityTypes.first().uppercase()),
            globalRoles = roles
        )

        return subjectReferentialService.create(subjectReferential)
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

    private suspend fun deleteSubjectReferential(entityDeleteEvent: EntityDeleteEvent): Either<APIException, Unit> =
        subjectReferentialService.delete(entityDeleteEvent.entityId.extractSub())

    private suspend fun updateSubjectProfile(attributeAppendEvent: AttributeAppendEvent): Either<APIException, Unit> {
        val operationPayloadNode = mapper.readTree(attributeAppendEvent.operationPayload)
        val subjectUuid = attributeAppendEvent.entityId.extractSub()
        val updateResult =
            if (attributeAppendEvent.attributeName == AUTH_TERM_ROLES) {
                val newRoles = (operationPayloadNode[JSONLD_VALUE] as ArrayNode).map {
                    GlobalRole.forKey(it.asText())
                }.flattenOption()
                if (newRoles.isNotEmpty())
                    subjectReferentialService.setGlobalRoles(subjectUuid, newRoles)
                else
                    subjectReferentialService.resetGlobalRoles(subjectUuid)
            } else if (attributeAppendEvent.attributeName == AUTH_TERM_SID) {
                val serviceAccountId = operationPayloadNode[JSONLD_VALUE].asText()
                subjectReferentialService.addServiceAccountIdToClient(
                    subjectUuid,
                    serviceAccountId.extractSub()
                )
            } else if (attributeAppendEvent.attributeName == AUTH_TERM_IS_MEMBER_OF) {
                val groupId = operationPayloadNode["object"].asText()
                subjectReferentialService.addGroupMembershipToUser(
                    subjectUuid,
                    groupId.extractSub()
                )
            } else {
                BadRequestDataException("Received unknown attribute name: ${attributeAppendEvent.attributeName}").left()
            }

        return updateResult
    }

    private suspend fun removeSubjectFromGroup(attributeDeleteEvent: AttributeDeleteEvent): Either<APIException, Unit> =
        subjectReferentialService.removeGroupMembershipToUser(
            attributeDeleteEvent.entityId.extractSub(),
            attributeDeleteEvent.datasetId!!.extractSub()
        )

    private suspend fun handleAttributeReplaceOnRights(
        attributeReplaceEvent: AttributeReplaceEvent
    ): Either<APIException, Unit> =
        if (attributeReplaceEvent.attributeName == AUTH_TERM_SAP) {
            updateSpecificAccessPolicy(attributeReplaceEvent.operationPayload, attributeReplaceEvent.entityId)
        } else {
            addEntityToSubject(
                attributeReplaceEvent.operationPayload,
                attributeReplaceEvent.attributeName,
                attributeReplaceEvent.entityId
            )
        }

    private suspend fun handleAttributeAppendOnRights(
        attributeAppendEvent: AttributeAppendEvent
    ): Either<APIException, Unit> =
        if (attributeAppendEvent.attributeName == AUTH_TERM_SAP) {
            updateSpecificAccessPolicy(attributeAppendEvent.operationPayload, attributeAppendEvent.entityId)
        } else {
            addEntityToSubject(
                attributeAppendEvent.operationPayload,
                attributeAppendEvent.attributeName,
                attributeAppendEvent.entityId
            )
        }

    private suspend fun updateSpecificAccessPolicy(
        operationPayload: String,
        entityId: URI
    ): Either<APIException, Unit> {
        val operationPayloadNode = mapper.readTree(operationPayload)
        val policy = SpecificAccessPolicy.valueOf(operationPayloadNode["value"].asText())
        return temporalEntityAttributeService.updateSpecificAccessPolicy(entityId, policy)
    }

    private suspend fun addEntityToSubject(
        operationPayload: String,
        attributeName: String,
        subjectId: URI
    ): Either<APIException, Unit> {
        val operationPayloadNode = mapper.readTree(operationPayload)
        val entityId = operationPayloadNode["object"].asText()
        return when (val accessRight = AccessRight.forAttributeName(attributeName)) {
            is Some ->
                entityAccessRightsService.setRoleOnEntity(
                    subjectId.extractSub(),
                    entityId.toUri(),
                    accessRight.value
                )
            is None -> BadRequestDataException("Unable to extract a known access right from $attributeName").left()
        }
    }

    private suspend fun handleAttributeDeleteOnRights(
        attributeDeleteEvent: AttributeDeleteEvent
    ): Either<APIException, Unit> =
        if (attributeDeleteEvent.attributeName == AUTH_TERM_SAP) {
            removeSpecificAccessPolicy(attributeDeleteEvent)
        } else {
            removeEntityFromSubject(attributeDeleteEvent)
        }

    private suspend fun removeSpecificAccessPolicy(
        attributeDeleteEvent: AttributeDeleteEvent
    ): Either<APIException, Unit> =
        temporalEntityAttributeService.removeSpecificAccessPolicy(attributeDeleteEvent.entityId)

    private suspend fun removeEntityFromSubject(
        attributeDeleteEvent: AttributeDeleteEvent
    ): Either<APIException, Unit> =
        entityAccessRightsService.removeRoleOnEntity(
            attributeDeleteEvent.entityId.extractSub(),
            attributeDeleteEvent.attributeName.toUri()
        )
}
