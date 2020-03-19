package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.context.event.EventListener
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class RepositoryEventsListener(
    private val neo4jService: Neo4jService,
    private val resolver: BinderAwareChannelResolver
) {

    private val logger = LoggerFactory.getLogger(RepositoryEventsListener::class.java)
    private val mapper = jacksonObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    // TODO: For deserialization in other modules Jackson can be used with the entityEvent model that gives a payload and an updatedEntity in a proper json format
    @Async
    @EventListener
    fun handleRepositoryEvent(entityEvent: EntityEvent) {
        val channelName = "cim.entity.${entityEvent.entityType}"

        // TODO BinderAwareChannelResolver is deprecated but there is no clear migration path yet, wait for maturity
        val result = when (entityEvent.operationType) {
            EventType.CREATE -> sendCreateMessage(channelName, entityEvent.entityId, entityEvent.entityType, entityEvent.payload!!)
            EventType.UPDATE -> sendUpdateMessage(channelName, entityEvent.entityId, entityEvent.entityType, entityEvent.payload!!)
            else -> false
        }

        if (result)
            logger.debug("Entity ${entityEvent.entityId} sent to $channelName")
        else
            logger.warn("Unable to send entity ${entityEvent.entityId} to $channelName")
    }

    private fun sendCreateMessage(channelName: String, entityId: String, entityType: String, payload: String): Boolean {
        val data = mapOf("operationType" to EventType.CREATE.name,
            "entityId" to entityId,
            "entityType" to entityType,
            "payload" to payload
        )

        return resolver.resolveDestination(channelName)
            .send(MessageBuilder.createMessage(mapper.writeValueAsString(data),
                MessageHeaders(mapOf(MessageHeaders.ID to entityId))
            ))
    }

    private fun sendUpdateMessage(channelName: String, entityId: String, entityType: String, payload: String): Boolean {
        val entity = getEntityById(entityId)
        val data = mapOf("operationType" to EventType.UPDATE.name,
            "entityId" to entityId,
            "entityType" to entityType,
            "payload" to payload,
            "updatedEntity" to entity
        )

        return resolver.resolveDestination(channelName)
            .send(MessageBuilder.createMessage(mapper.writeValueAsString(data),
                MessageHeaders(mapOf(MessageHeaders.ID to entityId))
            ))
    }

    private fun getEntityById(entityId: String): String {
        val entity = neo4jService.getFullEntityById(entityId)
        return mapper.writeValueAsString(JsonLdProcessor.compact(entity.first, mapOf("@context" to entity.second), JsonLdOptions()))
    }
}
