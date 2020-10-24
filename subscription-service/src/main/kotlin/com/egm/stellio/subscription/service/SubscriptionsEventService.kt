package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntitiesEvent
import com.egm.stellio.shared.util.JsonLdUtils.mapper
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class SubscriptionsEventService(
    private val resolver: BinderAwareChannelResolver
) {

    private val subscriptionChannelName = "cim.subscription"

    @Async
    fun publishSubscriptionEvent(event: EntitiesEvent) =
        resolver.resolveDestination(subscriptionChannelName)
            .send(
                MessageBuilder.createMessage(
                    mapper.writeValueAsString(event),
                    MessageHeaders(mapOf(MessageHeaders.ID to event.entityId))
                )
            )
}
