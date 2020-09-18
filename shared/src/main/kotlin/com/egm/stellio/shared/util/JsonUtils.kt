package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.model.Subscription
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
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
}

fun String.toUri(): URI =
    URI.create(this)

fun List<String>.toListOfUri(): List<URI> =
    this.map { it.toUri() }

fun List<URI>.toListOfString(): List<String> =
    this.map { it.toString() }
