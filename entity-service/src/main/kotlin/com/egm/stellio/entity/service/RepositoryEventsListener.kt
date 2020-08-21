package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.context.event.EventListener
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class RepositoryEventsListener(
    private val entityService: EntityService,
    private val resolver: BinderAwareChannelResolver
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper =
        jacksonObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    // TODO: For deserialization in other modules Jackson can be used with the entityEvent model that gives a payload and an updatedEntity in a proper json format
    @Async
    @EventListener
    fun handleRepositoryEvent(entityEvent: EntityEvent) {

        // TODO BinderAwareChannelResolver is deprecated but there is no clear migration path yet, wait for maturity
        val result = when (entityEvent.operationType) {
            EventType.CREATE -> sendCreateMessage(
                entityEvent.entityId,
                entityEvent.entityType!!,
                entityEvent.payload!!
            )
            EventType.UPDATE -> sendUpdateMessage(
                entityEvent.entityId,
                entityEvent.payload!!
            )
            else -> false
        }

        if (result)
            logger.debug("Entity ${entityEvent.entityId} successfully sent")
        else
            logger.warn("Unable to send entity ${entityEvent.entityId}")
    }

    private fun sendCreateMessage(entityId: String, entityType: String, payload: String): Boolean {
        val data = mapOf(
            "operationType" to EventType.CREATE.name,
            "entityId" to entityId,
            "entityType" to entityType,
            "payload" to payload
        )

        return resolver.resolveDestination(channelName(entityType))
            .send(
                MessageBuilder.createMessage(
                    mapper.writeValueAsString(data),
                    MessageHeaders(mapOf(MessageHeaders.ID to entityId))
                )
            )
    }

    private fun sendUpdateMessage(entityId: String, payload: String): Boolean {
        val jsonLdEntity = entityService.getFullEntityById(entityId)
        jsonLdEntity?.let {
            val entityType = jsonLdEntity.type.extractShortTypeFromExpanded()
            val data = mapOf(
                "operationType" to EventType.UPDATE.name,
                "entityId" to entityId,
                "entityType" to entityType,
                "payload" to payload,
                "updatedEntity" to compactAndSerialize(jsonLdEntity)
            )

            return resolver.resolveDestination(channelName(entityType))
                .send(
                    MessageBuilder.createMessage(
                        mapper.writeValueAsString(data),
                        MessageHeaders(mapOf(MessageHeaders.ID to entityId))
                    )
                )
        }
        return false
    }

    private fun channelName(entityType: String) =
        "cim.entity.$entityType"
}
