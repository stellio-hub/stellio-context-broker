package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.NotificationEvent
import com.egm.stellio.shared.model.SubscriptionEvent
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class SubscriptionEventService(
    private val resolver: BinderAwareChannelResolver
) {
    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    private val subscriptionChannelName = "cim.subscription"
    private val notificationChannelName = "cim.notification"

    @Async
    fun publishSubscriptionEvent(event: SubscriptionEvent) =
        resolver.resolveDestination(subscriptionChannelName)
            .send(
                MessageBuilder.createMessage(
                    mapper.writeValueAsString(event),
                    MessageHeaders(mapOf(MessageHeaders.ID to event.subscriptionId))
                )
            )

    @Async
    fun publishNotificationEvent(event: NotificationEvent) =
        resolver.resolveDestination(notificationChannelName)
            .send(
                MessageBuilder.createMessage(
                    mapper.writeValueAsString(event),
                    MessageHeaders(mapOf(MessageHeaders.ID to event.notificationId))
                )
            )
}
