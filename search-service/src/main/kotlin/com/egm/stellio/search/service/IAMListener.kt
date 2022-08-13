package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.flattenOption
import arrow.core.left
import com.egm.stellio.search.authorization.SubjectReferentialService
import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class IAMListener(
    private val subjectReferentialService: SubjectReferentialService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    @KafkaListener(topics = ["cim.iam"], groupId = "search-iam")
    fun processIam(content: String) {
        logger.debug("Received IAM event: $content")
        coroutineScope.launch {
            dispatchIamMessage(content)
        }
    }

    internal suspend fun dispatchIamMessage(content: String) {
        val authorizationEvent = JsonUtils.deserializeAs<EntityEvent>(content)
        kotlin.runCatching {
            when (authorizationEvent) {
                is EntityCreateEvent -> createSubjectReferential(authorizationEvent)
                is EntityDeleteEvent -> deleteSubjectReferential(authorizationEvent)
                is AttributeAppendEvent -> updateSubjectProfile(authorizationEvent)
                is AttributeDeleteEvent -> removeSubjectFromGroup(authorizationEvent)
                else ->
                    OperationNotSupportedException(unhandledOperationType(authorizationEvent.operationType)).left()
            }.fold({
                if (it is OperationNotSupportedException)
                    logger.info(it.message)
                else
                    logger.error(authorizationEvent.failedHandlingMessage(it))
            }, {
                logger.debug(authorizationEvent.successfulHandlingMessage())
            })
        }.onFailure {
            logger.error(authorizationEvent.failedHandlingMessage(it))
        }
    }

    @SuppressWarnings("unused")
    private suspend fun createFullSubjectReferential(
        entityCreateEvent: EntityCreateEvent
    ): Either<APIException, Unit> {
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

        return subjectReferentialService.create(subjectReferential)
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
}
