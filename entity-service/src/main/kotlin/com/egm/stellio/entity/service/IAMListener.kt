package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.parseEntityEvent
import com.egm.stellio.shared.util.toUri
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
        val entityEvent: EntityEvent = parseEntityEvent(content)

        when (entityEvent.operationType) {
            EventType.CREATE -> create(entityEvent)
            EventType.APPEND -> append(entityEvent)
            EventType.UPDATE -> update(entityEvent)
            EventType.DELETE -> delete(entityEvent)
            else -> logger.info("Entity event ${entityEvent.operationType} not handled.")
        }
    }

    private fun create(entityEvent: EntityEvent) {
        val ngsiLdEntity = expandJsonLdEntity(
            entityEvent.payload!!,
            entityEvent.context!!
        ).toNgsiLdEntity()

        entityService.createEntity(ngsiLdEntity)
    }

    private fun append(entityEvent: EntityEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(entityEvent.payload!!, entityEvent.context!!)

        entityService.appendEntityAttributes(
            entityEvent.entityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment),
            false
        )
    }

    private fun update(entityEvent: EntityEvent) {
        val expandedJsonLdFragment =
            expandJsonLdFragment(entityEvent.payload!!, entityEvent.context!!)

        entityService.updateEntityAttributes(
            entityEvent.entityId,
            parseToNgsiLdAttributes(expandedJsonLdFragment)
        )
    }

    private fun delete(entityEvent: EntityEvent) {
        if (entityEvent.attributeName != null) {
            entityService.deleteEntityAttributeInstance(
                entityEvent.entityId,
                expandJsonLdKey(entityEvent.attributeName!!, entityEvent.context!!)!!,
                entityEvent.datasetId!!.toUri()
            )
        } else {
            entityService.deleteEntity(entityEvent.entityId)
        }
    }
}
