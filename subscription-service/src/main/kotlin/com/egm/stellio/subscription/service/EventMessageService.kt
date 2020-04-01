package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.util.ApiUtils.serializeObject
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Component

@Component
class EventMessageService(
    private val resolver: BinderAwareChannelResolver
) {

    fun sendMessage(channelName: String, eventType: EventType, entityId: String, entityType: String, payload: String?, updatedEntity: String?): Boolean {
        val data = mapOf("operationType" to eventType.name,
            "entityId" to entityId,
            "entityType" to entityType,
            "payload" to payload,
            "updatedEntity" to updatedEntity
        )

        // TODO BinderAwareChannelResolver is deprecated but there is no clear migration path yet, wait for maturity
        return resolver.resolveDestination(channelName)
            .send(
                MessageBuilder.createMessage(
                    serializeObject(data),
                    MessageHeaders(mapOf(MessageHeaders.ID to entityId))
                ))
    }
}
