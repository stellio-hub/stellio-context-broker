package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.model.Subscription
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object JsonUtils {

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    fun parseEntityEvent(input: String): EntityEvent =
        mapper.readValue(input, EntityEvent::class.java)

    fun parseSubscription(content: String): Subscription =
        mapper.readValue(content, Subscription::class.java)

    fun parseNotification(content: String): Notification =
        mapper.readValue(content, Notification::class.java)

    fun parseJsonContent(content: String): JsonNode =
        mapper.readTree(content)

    fun serializeObject(input: Any): String =
        mapper.writeValueAsString(input)
}
