package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.reflect.KClass

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

    fun parseListOfEntities(content: String): List<Map<String, Any>> =
        mapper.readValue(
            content,
            mapper.typeFactory.constructCollectionType(MutableList::class.java, Map::class.java)
        )

    fun serializeObject(input: Any): String =
        mapper.writeValueAsString(input)

    fun serializeObject(
        input: Any,
        inputClass: KClass<*>,
        filterMixin: KClass<*>,
        filterMixinName: String,
        propertiesToExclude: Set<String>
    ): String {
        val mapperWithMixin = mapper.copy()
        mapperWithMixin.addMixIn(inputClass.java, filterMixin.java)
        val filterProvider = SimpleFilterProvider().addFilter(
            filterMixinName,
            SimpleBeanPropertyFilter.serializeAllExcept(propertiesToExclude)
        )
        return mapperWithMixin.writer(filterProvider).writeValueAsString(input)
    }

    fun parseEntitiesEvent(input: String): EntitiesEvent =
        mapper.readValue(input, EntitiesEvent::class.java)

    fun parseSubscriptionEvent(input: String): SubscriptionEvent =
        mapper.readValue(input, SubscriptionEvent::class.java)

    fun parseNotificationEvent(input: String): NotificationEvent =
        mapper.readValue(input, NotificationEvent::class.java)
}
