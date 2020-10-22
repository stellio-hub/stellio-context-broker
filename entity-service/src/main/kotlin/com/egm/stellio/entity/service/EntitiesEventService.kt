package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EntitiesEvent
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class EntitiesEventService(
    private val resolver: BinderAwareChannelResolver
) {

    private val mapper =
        jacksonObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)

    @Async
    // only keep the event in the method constructor, re-add entityType to EntitiesEvent model ?
    fun publishEntityEvent(event: EntitiesEvent, channelSuffix: String) =
        resolver.resolveDestination(entityChannelName(channelSuffix))
            .send(
                MessageBuilder.createMessage(
                    mapper.writeValueAsString(event),
                    MessageHeaders(mapOf(MessageHeaders.ID to event.entityId))
                )
            )

    private fun entityChannelName(entityType: String) =
        "cim.entity.$entityType"
}
