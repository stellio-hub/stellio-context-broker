package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.flattenOption
import arrow.core.left
import com.egm.stellio.search.authorization.SubjectReferential
import com.egm.stellio.search.authorization.SubjectReferentialService
import com.egm.stellio.search.authorization.toSubjectInfo
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_SUBJECT_INFO_MEMBERS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SID
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.extractSub
import io.r2dbc.postgresql.codec.Json
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
                is AttributeReplaceEvent -> updateSubjectInfo(authorizationEvent)
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

    private suspend fun createSubjectReferential(entityCreateEvent: EntityCreateEvent): Either<APIException, Unit> {
        val operationPayload = entityCreateEvent.operationPayload.deserializeAsMap()
        val subjectInfo = operationPayload.filter { AUTH_SUBJECT_INFO_MEMBERS.contains(it.key) }.toSubjectInfo()
        val roles = extractRoles(operationPayload)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSub(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityTypes.first().uppercase()),
            subjectInfo = Json.of(subjectInfo),
            globalRoles = roles
        )

        return subjectReferentialService.create(subjectReferential)
    }

    private fun extractRoles(operationPayload: Map<String, Any>): List<GlobalRole>? =
        if (operationPayload.containsKey(AUTH_TERM_ROLES)) {
            when (val rolesValue = (operationPayload[AUTH_TERM_ROLES] as Map<String, Any>)[JSONLD_VALUE]) {
                is String -> GlobalRole.forKey(rolesValue).map { listOf(it) }.orNull()
                is List<*> -> rolesValue.map { GlobalRole.forKey(it as String) }.flattenOption()
                else -> null
            }
        } else null

    private suspend fun deleteSubjectReferential(entityDeleteEvent: EntityDeleteEvent): Either<APIException, Unit> =
        subjectReferentialService.delete(entityDeleteEvent.entityId.extractSub())

    private suspend fun updateSubjectProfile(attributeAppendEvent: AttributeAppendEvent): Either<APIException, Unit> {
        val operationPayload = attributeAppendEvent.operationPayload.deserializeAsMap()
        val subjectUuid = attributeAppendEvent.entityId.extractSub()
        val updateResult =
            if (attributeAppendEvent.attributeName == AUTH_TERM_ROLES) {
                val newRoles = (operationPayload[JSONLD_VALUE] as List<*>).map {
                    GlobalRole.forKey(it as String)
                }.flattenOption()
                if (newRoles.isNotEmpty())
                    subjectReferentialService.setGlobalRoles(subjectUuid, newRoles)
                else
                    subjectReferentialService.resetGlobalRoles(subjectUuid)
            } else if (attributeAppendEvent.attributeName == AUTH_TERM_SID) {
                val serviceAccountId = operationPayload[JSONLD_VALUE] as String
                subjectReferentialService.addServiceAccountIdToClient(
                    subjectUuid,
                    serviceAccountId.extractSub()
                )
            } else if (attributeAppendEvent.attributeName == AUTH_TERM_IS_MEMBER_OF) {
                val groupId = operationPayload[JSONLD_OBJECT] as String
                subjectReferentialService.addGroupMembershipToUser(
                    subjectUuid,
                    groupId.extractSub()
                )
            } else {
                BadRequestDataException(
                    "Received unknown attribute name: ${attributeAppendEvent.attributeName}"
                ).left()
            }

        return updateResult
    }

    private suspend fun updateSubjectInfo(attributeReplaceEvent: AttributeReplaceEvent): Either<APIException, Unit> {
        val operationPayload = attributeReplaceEvent.operationPayload.deserializeAsMap()
        val subjectUuid = attributeReplaceEvent.entityId.extractSub()
        val newSubjectInfo = Pair(attributeReplaceEvent.attributeName, operationPayload[JSONLD_VALUE] as String)

        return subjectReferentialService.updateSubjectInfo(
            subjectUuid,
            newSubjectInfo
        )
    }

    private suspend fun removeSubjectFromGroup(attributeDeleteEvent: AttributeDeleteEvent): Either<APIException, Unit> =
        subjectReferentialService.removeGroupMembershipToUser(
            attributeDeleteEvent.entityId.extractSub(),
            attributeDeleteEvent.datasetId!!.extractSub()
        )
}
