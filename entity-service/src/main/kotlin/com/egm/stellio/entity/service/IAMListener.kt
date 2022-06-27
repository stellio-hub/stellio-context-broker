package com.egm.stellio.entity.service

import com.egm.stellio.entity.authorization.Neo4jAuthorizationRepository
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_ROLES
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class IAMListener(
    private val entityService: EntityService,
    private val neo4jAuthorizationRepository: Neo4jAuthorizationRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["cim.iam"], groupId = "entity-iam")
    fun processMessage(content: String) {
        logger.debug("Received event: $content")
        when (val authorizationEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> create(authorizationEvent)
            is EntityDeleteEvent -> delete(authorizationEvent)
            is AttributeAppendEvent -> append(authorizationEvent)
            is AttributeReplaceEvent -> update(authorizationEvent)
            is AttributeDeleteEvent -> deleteAttribute(authorizationEvent)
            else -> logger.info("Authorization event ${authorizationEvent.operationType} not handled.")
        }
    }

    private fun create(authorizationEvent: EntityCreateEvent) {
        val ngsiLdEntity = expandJsonLdEntity(
            authorizationEvent.operationPayload,
            authorizationEvent.contexts
        ).toNgsiLdEntity()

        entityService.createEntity(ngsiLdEntity)
    }

    private fun append(authorizationEvent: AttributeAppendEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(
                authorizationEvent.attributeName,
                authorizationEvent.operationPayload,
                authorizationEvent.contexts
            )

        // events received when adding a service account to a group are using the service account id as subject
        // so get first the eventual client id in case this is such an event
        val subjectEntityId =
            if (authorizationEvent.attributeName == AUTH_TERM_IS_MEMBER_OF)
                neo4jAuthorizationRepository.getSubjectUri(authorizationEvent.entityId)
            else authorizationEvent.entityId

        entityService.appendEntityAttributes(
            subjectEntityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment),
            false
        ).also {
            if (it.isSuccessful()) {
                when (authorizationEvent.attributeName) {
                    AUTH_TERM_ROLES ->
                        neo4jAuthorizationRepository.resetRolesCache()
                    AUTH_TERM_IS_MEMBER_OF ->
                        neo4jAuthorizationRepository.updateSubjectGroups(subjectEntityId)
                }
            }
        }
    }

    private fun update(authorizationEvent: AttributeReplaceEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(
                authorizationEvent.attributeName,
                authorizationEvent.operationPayload,
                authorizationEvent.contexts
            )

        entityService.updateEntityAttributes(
            authorizationEvent.entityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment)
        )
    }

    private fun delete(authorizationEvent: EntityDeleteEvent) {
        entityService.deleteEntity(authorizationEvent.entityId)
            .also {
                if (it.first > 0)
                    neo4jAuthorizationRepository.evictSubject(authorizationEvent.entityId)
            }
    }

    private fun deleteAttribute(authorizationEvent: AttributeDeleteEvent) =
        entityService.deleteEntityAttributeInstance(
            authorizationEvent.entityId,
            expandJsonLdTerm(
                authorizationEvent.attributeName,
                authorizationEvent.contexts
            )!!,
            authorizationEvent.datasetId
        ).also {
            if (it) {
                when (authorizationEvent.attributeName) {
                    AUTH_TERM_IS_MEMBER_OF ->
                        neo4jAuthorizationRepository.updateSubjectGroups(authorizationEvent.entityId)
                }
            }
        }
}
