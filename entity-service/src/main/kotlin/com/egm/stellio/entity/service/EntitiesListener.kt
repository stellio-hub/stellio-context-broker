package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getContextOrThrowError
import com.egm.stellio.shared.util.NgsiLdParsingUtils.parseEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils.parseEntityEvent
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component
import java.net.URI

@Component
@EnableBinding(EntityEventSink::class)
class EntitiesListener(
    private val entityService: EntityService
) {
    private val logger = LoggerFactory.getLogger(EntitiesListener::class.java)

    @StreamListener("cim.entities")
    fun processMessage(content: String) {
        val entityEvent: EntityEvent = parseEntityEvent(content)

        when (entityEvent.operationType) {
            EventType.CREATE -> create(entityEvent)
            EventType.APPEND -> append(entityEvent)
            EventType.DELETE -> delete(entityEvent)
            else -> logger.info("Entity event ${entityEvent.operationType} not handled.")
        }
    }

    private fun create(entityEvent: EntityEvent) {
        val expandedEntity = parseEntity(entityEvent.payload!!, getContextOrThrowError(entityEvent.payload!!))

        entityService.createEntity(expandedEntity)
    }

    private fun append(entityEvent: EntityEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(entityEvent.payload!!, getContextOrThrowError(entityEvent.payload!!))

        entityService.appendEntityAttributes(entityEvent.entityId, expandedJsonLdFragment, false)
    }

    private fun delete(entityEvent: EntityEvent) {
        if (entityEvent.attributeName != null) {
            entityService.deleteEntityAttributeInstance(
                entityEvent.entityId,
                entityEvent.attributeName!!,
                URI.create(entityEvent.datasetId!!),
                entityEvent.context!![0]
            )
        } else {
            entityService.deleteEntity(entityEvent.entityId)
        }
    }
}
