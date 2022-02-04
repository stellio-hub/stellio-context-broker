package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityUpdateEvent
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import java.net.URI

@Component
class SubscriptionEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val subscriptionService: SubscriptionService
) {

    private val subscriptionChannelName = "cim.subscription"
    private val notificationChannelName = "cim.notification"

    @Async
    suspend fun publishSubscriptionCreateEvent(
        subscriptionId: URI,
        contexts: List<String>
    ) {
        val subscription = subscriptionService.getById(subscriptionId).awaitFirst()
        val event = EntityCreateEvent(
            subscription.id,
            subscription.type,
            subscription.toJson(contexts, MediaType.APPLICATION_JSON, true),
            contexts
        )

        kafkaTemplate.send(subscriptionChannelName, event.entityId.toString(), serializeObject(event))
    }

    @Async
    suspend fun publishSubscriptionUpdateEvent(subscriptionId: URI, operationPayload: String, contexts: List<String>) {
        val subscription = subscriptionService.getById(subscriptionId).awaitFirst()
        val event = EntityUpdateEvent(
            subscriptionId,
            subscription.type,
            operationPayload,
            subscription.toJson(contexts, MediaType.APPLICATION_JSON, true),
            contexts
        )

        kafkaTemplate.send(subscriptionChannelName, event.entityId.toString(), serializeObject(event))
    }

    @Async
    fun publishSubscriptionDeleteEvent(subscriptionId: URI, contexts: List<String>) {
        val event = EntityDeleteEvent(
            subscriptionId,
            "Subscription",
            contexts
        )

        kafkaTemplate.send(subscriptionChannelName, event.entityId.toString(), serializeObject(event))
    }

    @Async
    fun publishNotificationCreateEvent(notification: Notification) {
        val event = EntityCreateEvent(
            notification.id,
            notification.type,
            serializeObject(notification),
            listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT)
        )

        kafkaTemplate.send(notificationChannelName, event.entityId.toString(), serializeObject(event))
    }
}
