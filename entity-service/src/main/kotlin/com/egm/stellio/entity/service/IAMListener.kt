package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.parseEntityEvent
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(IAMEventSink::class)
class IAMListener(
    private val entityService: EntityService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @StreamListener("cim.iam")
    fun processMessage(content: String) {
        when (val authorizationEvent = parseEntityEvent(content)) {
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
            expandJsonLdFragment(authorizationEvent.operationPayload, authorizationEvent.contexts)

        entityService.appendEntityAttributes(
            authorizationEvent.entityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment),
            false
        )
    }

    private fun update(authorizationEvent: AttributeReplaceEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(authorizationEvent.operationPayload, authorizationEvent.contexts)

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
