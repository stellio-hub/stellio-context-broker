package com.egm.stellio.search.listener

import arrow.core.Either
import arrow.core.flattenOption
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.search.authorization.SubjectReferential
import com.egm.stellio.search.authorization.SubjectReferentialService
import com.egm.stellio.search.authorization.toSubjectInfo
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SID
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.extractSub
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.net.URI

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
            val tenantUri = authorizationEvent.tenantUri
            when (authorizationEvent) {
                is EntityCreateEvent -> createSubjectReferential(tenantUri, authorizationEvent)
                is EntityDeleteEvent -> deleteSubjectReferential(tenantUri, authorizationEvent)
                is AttributeAppendEvent -> updateSubjectProfile(tenantUri, authorizationEvent)
                is AttributeReplaceEvent -> updateSubjectInfo(tenantUri, authorizationEvent)
                is AttributeDeleteEvent -> removeSubjectFromGroup(tenantUri, authorizationEvent)
                else ->
                    OperationNotSupportedException(unhandledOperationType(authorizationEvent.operationType)).left()
            }
        }.onFailure {
            logger.error(authorizationEvent.failedHandlingMessage(it))
        }
    }

    private suspend fun createSubjectReferential(
        tenantUri: URI,
        entityCreateEvent: EntityCreateEvent
    ): Either<APIException, Unit> = either {
        val operationPayload = entityCreateEvent.operationPayload.deserializeAsMap()
        val subjectInfo = operationPayload
            .filter { !JSONLD_COMPACTED_ENTITY_CORE_MEMBERS.contains(it.key) }
            .toSubjectInfo()
        val roles = extractRoles(operationPayload)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSub(),
            subjectType = SubjectType.valueOf(entityCreateEvent.entityTypes.first().uppercase()),
            subjectInfo = Json.of(subjectInfo),
            globalRoles = roles
        )

        mono {
            subjectReferentialService.create(subjectReferential)
        }.writeContextAndSubscribe(tenantUri, entityCreateEvent)
    }

    private fun extractRoles(operationPayload: Map<String, Any>): List<GlobalRole>? =
        if (operationPayload.containsKey(AUTH_TERM_ROLES)) {
            when (val rolesValue = (operationPayload[AUTH_TERM_ROLES] as Map<String, Any>)[JSONLD_VALUE_TERM]) {
                is String -> GlobalRole.forKey(rolesValue).map { listOf(it) }.getOrNull()
                is List<*> -> rolesValue.map { GlobalRole.forKey(it as String) }.flattenOption()
                else -> null
            }
        } else null

    private suspend fun deleteSubjectReferential(
        tenantUri: URI,
        entityDeleteEvent: EntityDeleteEvent
    ): Either<APIException, Unit> = either {
        mono {
            subjectReferentialService.delete(entityDeleteEvent.entityId.extractSub())
        }.writeContextAndSubscribe(tenantUri, entityDeleteEvent)
    }

    private suspend fun updateSubjectProfile(
        tenantUri: URI,
        attributeAppendEvent: AttributeAppendEvent
    ): Either<APIException, Unit> = either {
        val operationPayload = attributeAppendEvent.operationPayload.deserializeAsMap()
        val subjectUuid = attributeAppendEvent.entityId.extractSub()
        mono {
            if (attributeAppendEvent.attributeName == AUTH_TERM_ROLES) {
                val newRoles = (operationPayload[JSONLD_VALUE_TERM] as List<*>).map {
                    GlobalRole.forKey(it as String)
                }.flattenOption()
                if (newRoles.isNotEmpty())
                    subjectReferentialService.setGlobalRoles(subjectUuid, newRoles)
                else
                    subjectReferentialService.resetGlobalRoles(subjectUuid)
            } else if (attributeAppendEvent.attributeName == AUTH_TERM_SID) {
                val serviceAccountId = operationPayload[JSONLD_VALUE_TERM] as String
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
        }.writeContextAndSubscribe(tenantUri, attributeAppendEvent)
    }

    private suspend fun updateSubjectInfo(
        tenantUri: URI,
        attributeReplaceEvent: AttributeReplaceEvent
    ): Either<APIException, Unit> = either {
        val operationPayload = attributeReplaceEvent.operationPayload.deserializeAsMap()
        val subjectUuid = attributeReplaceEvent.entityId.extractSub()
        val newSubjectInfo = Pair(attributeReplaceEvent.attributeName, operationPayload[JSONLD_VALUE_TERM] as String)

        mono {
            subjectReferentialService.updateSubjectInfo(
                subjectUuid,
                newSubjectInfo
            )
        }.writeContextAndSubscribe(tenantUri, attributeReplaceEvent)
    }

    private suspend fun removeSubjectFromGroup(
        tenantUri: URI,
        attributeDeleteEvent: AttributeDeleteEvent
    ): Either<APIException, Unit> = either {
        mono {
            subjectReferentialService.removeGroupMembershipToUser(
                attributeDeleteEvent.entityId.extractSub(),
                attributeDeleteEvent.datasetId!!.extractSub()
            )
        }.writeContextAndSubscribe(tenantUri, attributeDeleteEvent)
    }

    private fun Mono<Either<APIException, Unit>>.writeContextAndSubscribe(
        tenantUri: URI,
        event: EntityEvent
    ) = this.contextWrite {
        it.put(NGSILD_TENANT_HEADER, tenantUri)
    }.subscribe {
        it.fold({ apiException ->
            if (apiException is OperationNotSupportedException)
                logger.info(apiException.message)
            else
                logger.error(event.failedHandlingMessage(apiException))
        }, {
            logger.debug(event.successfulHandlingMessage())
        })
    }
}
