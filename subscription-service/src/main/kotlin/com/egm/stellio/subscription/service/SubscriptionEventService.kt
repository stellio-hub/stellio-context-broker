package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntityEvent
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class SubscriptionEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    private val subscriptionChannelName = "cim.subscription"
    private val notificationChannelName = "cim.notification"

    fun publishSubscriptionEvent(event: EntityEvent) =
        kafkaTemplate.send(subscriptionChannelName, event.entityId.toString(), mapper.writeValueAsString(event))

    fun publishNotificationEvent(event: EntityEvent) =
        kafkaTemplate.send(notificationChannelName, event.entityId.toString(), mapper.writeValueAsString(event))
}
