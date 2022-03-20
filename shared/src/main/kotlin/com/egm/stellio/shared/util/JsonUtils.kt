package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.InvalidRequestException
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.reflect.KClass

val mapper: ObjectMapper =
    jacksonObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

object JsonUtils {

    inline fun <reified T> deserializeAs(content: String): T =
        mapper.readValue(content, T::class.java)

    @SuppressWarnings("SwallowedException")
    fun deserializeObject(input: String): Map<String, Any> =
        try {
            mapper.readValue(
                input,
                mapper.typeFactory.constructMapLikeType(Map::class.java, String::class.java, Any::class.java)
            )
        } catch (e: JsonProcessingException) {
            throw InvalidRequestException(e.message!!)
        }

    fun String.deserializeAsMap(): Map<String, Any> =
        deserializeObject(this)

    fun deserializeListOfObjects(input: String): List<Map<String, Any>> =
        mapper.readValue(
            input,
            mapper.typeFactory.constructCollectionType(MutableList::class.java, Map::class.java)
        )

    fun String.deserializeAsList(): List<Map<String, Any>> =
        deserializeListOfObjects(this)

    fun serializeObject(input: Any): String =
        mapper.writeValueAsString(input)

    fun Map<String, Any>.serialize(): String =
        mapper.writeValueAsString(this)

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
