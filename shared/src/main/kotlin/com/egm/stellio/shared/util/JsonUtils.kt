package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_JSON_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.reflect.KClass

val mapper: ObjectMapper =
    jacksonObjectMapper()
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

// a specific object mapper for data types defined in 5.2
// difference is that unknown properties are not allowed (in contrast to entities)
val dataTypeMapper: ObjectMapper =
    jacksonObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

object JsonUtils {

    inline fun <reified T> deserializeAs(content: String): T =
        mapper.readValue(content, T::class.java)

    inline fun <reified T> deserializeDataTypeAs(content: String): T =
        dataTypeMapper.readValue(content, T::class.java)

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

    @Suppress("UNCHECKED_CAST")
    fun String.deserializeExpandedPayload(): Map<String, List<Any>> =
        deserializeObject(this) as Map<String, List<Any>>

    @SuppressWarnings("SwallowedException")
    fun deserializeListOfObjects(input: String): List<Map<String, Any>> =
        try {
            mapper.readValue(
                input,
                mapper.typeFactory.constructCollectionType(MutableList::class.java, Map::class.java)
            )
        } catch (e: JsonProcessingException) {
            throw InvalidRequestException(e.message!!)
        }

    fun String.deserializeAsList(): List<Map<String, Any>> =
        deserializeListOfObjects(this)

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

    fun Map<String, Any>.getAllKeys(): Set<String> =
        this.entries.fold(emptySet()) { acc, entry ->
            // what is inside the value of a property is not a key
            if (entry.key == JSONLD_VALUE_TERM)
                acc.plus(entry.key)
            else {
                val valueKeys = when (entry.value) {
                    is Map<*, *> -> (entry.value as Map<String, Any>).getAllKeys()
                    is List<*> ->
                        (entry.value as List<Any>).map {
                            // type value can be a list, not interested in it here
                            if (it is Map<*, *>)
                                (it as Map<String, Any>).getAllKeys()
                            else emptySet()
                        }.flatten().toSet()
                    // if it is not a list or an object, it is a value (and thus not a key)
                    else -> emptySet()
                }
                acc.plus(entry.key).plus(valueKeys)
            }
        }

    fun Map<String, Any>.getAllValues(): Set<Any?> =
        this.entries.fold(emptySet()) { acc, entry ->
            val values = when {
                entry.value is Map<*, *> &&
                    entry.key in listOf(JSONLD_VALUE_TERM, JSONLD_JSON_TERM) -> setOf(entry.value)
                entry.value is Map<*, *> -> (entry.value as Map<String, Any>).getAllValues()
                entry.value is List<*> ->
                    (entry.value as List<Any>).map {
                        when (it) {
                            is Map<*, *> -> (it as Map<String, Any>).getAllValues()
                            else -> setOf(it)
                        }
                    }.flatten().toSet()
                else -> setOf(entry.value)
            }
            acc.plus(values)
        }
}
