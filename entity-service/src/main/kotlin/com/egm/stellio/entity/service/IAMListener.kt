package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class IAMListener(
    private val entityService: EntityService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["cim.iam"], groupId = "entity-iam")
    fun processMessage(content: String) {
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
                mapOf(authorizationEvent.attributeName to deserializeObject(authorizationEvent.operationPayload)),
                authorizationEvent.contexts
            )

        entityService.appendEntityAttributes(
            authorizationEvent.entityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment),
            false
        )
    }

    private fun update(authorizationEvent: AttributeReplaceEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(
                mapOf(authorizationEvent.attributeName to deserializeObject(authorizationEvent.operationPayload)),
                authorizationEvent.contexts
            )

        entityService.updateEntityAttributes(
            authorizationEvent.entityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment)
        )
    }

    private fun delete(authorizationEvent: EntityDeleteEvent) = entityService.deleteEntity(authorizationEvent.entityId)

    private fun deleteAttribute(authorizationEvent: AttributeDeleteEvent) =
        entityService.deleteEntityAttributeInstance(
            authorizationEvent.entityId,
            expandJsonLdKey(
                authorizationEvent.attributeName,
                authorizationEvent.contexts
            )!!,
            authorizationEvent.datasetId
        )
}
