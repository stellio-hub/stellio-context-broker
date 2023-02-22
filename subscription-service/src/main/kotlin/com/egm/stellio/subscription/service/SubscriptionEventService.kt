package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityUpdateEvent
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NOTIFICATION_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.net.URI

@Component
class SubscriptionEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val subscriptionService: SubscriptionService
) {

    private val subscriptionChannelName = "cim.subscription"
    private val notificationChannelName = "cim.notification"

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    fun publishSubscriptionCreateEvent(
        sub: String?,
        subscriptionId: URI,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            val subscription = subscriptionService.getById(subscriptionId)
            val event = EntityCreateEvent(
                sub,
                subscription.id,
                listOf(NGSILD_SUBSCRIPTION_TERM),
                subscription.serialize(contexts, MediaType.APPLICATION_JSON, true),
                contexts
            )

            kafkaTemplate.send(subscriptionChannelName, event.entityId.toString(), serializeObject(event))
        }

    fun publishSubscriptionUpdateEvent(
        sub: String?,
        subscriptionId: URI,
        operationPayload: String,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            val subscription = subscriptionService.getById(subscriptionId)
            val event = EntityUpdateEvent(
                sub,
                subscriptionId,
                listOf(NGSILD_SUBSCRIPTION_TERM),
                operationPayload,
                subscription.serialize(contexts, MediaType.APPLICATION_JSON, true),
                contexts
            )

            kafkaTemplate.send(subscriptionChannelName, event.entityId.toString(), serializeObject(event))
        }

    fun publishSubscriptionDeleteEvent(sub: String?, subscriptionId: URI, contexts: List<String>): Job =
        coroutineScope.launch {
            val event = EntityDeleteEvent(
                sub,
                subscriptionId,
                listOf(NGSILD_SUBSCRIPTION_TERM),
                contexts
            )

            kafkaTemplate.send(subscriptionChannelName, event.entityId.toString(), serializeObject(event))
        }

    fun publishNotificationCreateEvent(sub: String?, notification: Notification): Job =
        coroutineScope.launch {
            val event = EntityCreateEvent(
                sub,
                notification.id,
                listOf(NGSILD_NOTIFICATION_TERM),
                serializeObject(notification),
                listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT)
            )

            kafkaTemplate.send(notificationChannelName, event.entityId.toString(), serializeObject(event))
        }
}
