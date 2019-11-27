package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.EntityEvent
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.context.event.EventListener
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class RepositoryEventsListener(
    private val resolver: BinderAwareChannelResolver
) {

    private val logger = LoggerFactory.getLogger(RepositoryEventsListener::class.java)

    @Async
    @EventListener
    fun handleRepositoryEvent(entityEvent: EntityEvent) {
        val channelName = "cim.entity.${entityEvent.entityType}"
        // TODO BinderAwareChannelResolver is deprecated but there is no clear migration path yet, wait for maturity
        val result = resolver.resolveDestination(channelName)
            .send(MessageBuilder.createMessage(entityEvent.payload,
                MessageHeaders(mapOf(MessageHeaders.ID to entityEvent.entityUrn))
            ))

        if (result)
            logger.debug("Entity ${entityEvent.entityUrn} sent to $channelName")
        else
            logger.warn("Unable to send entity ${entityEvent.entityUrn} to $channelName")
    }
}
