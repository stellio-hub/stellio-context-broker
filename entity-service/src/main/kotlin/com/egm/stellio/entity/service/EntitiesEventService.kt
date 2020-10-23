package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EntitiesEvent
import com.egm.stellio.shared.util.JsonLdUtils.mapper
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class EntitiesEventService(
    private val resolver: BinderAwareChannelResolver
) {

    @Async
    fun publishEntityEvent(event: EntitiesEvent, channelSuffix: String) =
        resolver.resolveDestination(entityChannelName(channelSuffix))
            .send(
                MessageBuilder.createMessage(
                    mapper.writeValueAsString(event),
                    MessageHeaders(mapOf(MessageHeaders.ID to event.entityId))
                )
            )

    private fun entityChannelName(channelSuffix: String) =
        "cim.entity.$channelSuffix"
}
