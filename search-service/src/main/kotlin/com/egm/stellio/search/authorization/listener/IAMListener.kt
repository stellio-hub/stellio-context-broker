package com.egm.stellio.search.authorization.listener

import arrow.core.Either
import arrow.core.flattenOption
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.model.SubjectReferential
import com.egm.stellio.search.authorization.model.toSubjectInfo
import com.egm.stellio.search.authorization.service.EntityAccessRightsService
import com.egm.stellio.search.authorization.service.SubjectReferentialService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.OperationNotSupportedException
import com.egm.stellio.shared.model.unhandledOperationType
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_ROLES
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

@Component
class IAMListener(
    private val subjectReferentialService: SubjectReferentialService,
    private val searchProperties: SearchProperties,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val entityService: EntityService
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
            val tenantName = authorizationEvent.tenantName
            when (authorizationEvent) {
                is EntityCreateEvent -> createSubjectReferential(tenantName, authorizationEvent)
                is EntityDeleteEvent -> deleteSubjectReferential(tenantName, authorizationEvent)
                is AttributeAppendEvent -> updateSubjectProfile(tenantName, authorizationEvent)
                is AttributeReplaceEvent -> updateSubjectInfo(tenantName, authorizationEvent)
                is AttributeDeleteEvent -> removeSubjectFromGroup(tenantName, authorizationEvent)
                else ->
                    OperationNotSupportedException(unhandledOperationType(authorizationEvent.operationType)).left()
            }
        }.onFailure {
            logger.error(authorizationEvent.failedHandlingMessage(it))
        }
    }

    private suspend fun createSubjectReferential(
        tenantName: String,
        entityCreateEvent: EntityCreateEvent
    ): Either<APIException, Unit> = either {
        val subjectType = SubjectType.valueOf(entityCreateEvent.entityTypes.first().uppercase())
        val operationPayload = entityCreateEvent.operationPayload.deserializeAsMap()
        val subjectInfo = operationPayload
            .filter { !JSONLD_COMPACTED_ENTITY_CORE_MEMBERS.contains(it.key) }
            .toSubjectInfo()
        val roles = extractRoles(operationPayload)
        val subjectReferential = SubjectReferential(
            subjectId = entityCreateEvent.entityId.extractSub(),
            subjectType = subjectType,
            subjectInfo = Json.of(subjectInfo),
            globalRoles = roles
        )

        mono {
            if (subjectType == SubjectType.CLIENT)
                subjectReferentialService.upsertClient(subjectReferential)
            else
                subjectReferentialService.create(subjectReferential)
        }.writeContextAndSubscribe(tenantName, entityCreateEvent)
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
        tenantName: String,
        entityDeleteEvent: EntityDeleteEvent
    ): Either<APIException, Unit> = either {
        val subjectType = SubjectType.valueOf(entityDeleteEvent.entityTypes.first().uppercase())
        val sub = entityDeleteEvent.entityId.extractSub()
        mono {
            // delete the entities owned by the user while the user still exists
            // (if it no longer exists, it fails because of access rights checks)
            if (searchProperties.onOwnerDeleteCascadeEntities && subjectType == SubjectType.USER) {
                entityAccessRightsService.getEntitiesIdsOwnedBySubject(sub).getOrNull()?.forEach { entityId ->
                    entityService.permanentlyDeleteEntity(entityId, sub)
                }
                Unit.right()
            } else Unit.right()
            subjectReferentialService.delete(entityDeleteEvent.entityId.extractSub())
        }.writeContextAndSubscribe(tenantName, entityDeleteEvent)
    }

    private suspend fun updateSubjectProfile(
        tenantName: String,
        attributeAppendEvent: AttributeAppendEvent
    ): Either<APIException, Unit> = either {
        val operationPayload = attributeAppendEvent.operationPayload.deserializeAsMap()
        val subjectUuid = attributeAppendEvent.entityId.extractSub()
        mono {
            when (attributeAppendEvent.attributeName) {
                AUTH_TERM_ROLES -> {
                    val newRoles = (operationPayload[JSONLD_VALUE_TERM] as List<*>).map {
                        GlobalRole.forKey(it as String)
                    }.flattenOption()
                    if (newRoles.isNotEmpty())
                        subjectReferentialService.setGlobalRoles(subjectUuid, newRoles)
                    else
                        subjectReferentialService.resetGlobalRoles(subjectUuid)
                }
                AUTH_TERM_IS_MEMBER_OF -> {
                    val groupId = operationPayload[JSONLD_OBJECT] as String
                    subjectReferentialService.addGroupMembershipToUser(
                        subjectUuid,
                        groupId.extractSub()
                    )
                }
                else ->
                    BadRequestDataException(
                        "Received unknown attribute name: ${attributeAppendEvent.attributeName}"
                    ).left()
            }
        }.writeContextAndSubscribe(tenantName, attributeAppendEvent)
    }

    private suspend fun updateSubjectInfo(
        tenantName: String,
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
        }.writeContextAndSubscribe(tenantName, attributeReplaceEvent)
    }

    private suspend fun removeSubjectFromGroup(
        tenantName: String,
        attributeDeleteEvent: AttributeDeleteEvent
    ): Either<APIException, Unit> = either {
        mono {
            subjectReferentialService.removeGroupMembershipToUser(
                attributeDeleteEvent.entityId.extractSub(),
                attributeDeleteEvent.datasetId!!.extractSub()
            )
        }.writeContextAndSubscribe(tenantName, attributeDeleteEvent)
    }

    private fun Mono<Either<APIException, Unit>>.writeContextAndSubscribe(
        tenantName: String,
        event: EntityEvent
    ) = this.contextWrite {
        it.put(NGSILD_TENANT_HEADER, tenantName)
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
