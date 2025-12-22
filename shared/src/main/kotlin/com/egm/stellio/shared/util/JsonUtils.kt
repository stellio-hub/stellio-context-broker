package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.COMPACTED_ATTRIBUTES_TERMS
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.model.NGSILD_JSON_TERM
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper
import tools.jackson.databind.ser.std.SimpleBeanPropertyFilter
import tools.jackson.databind.ser.std.SimpleFilterProvider
import tools.jackson.module.kotlin.jsonMapper
import tools.jackson.module.kotlin.kotlinModule
import kotlin.reflect.KClass

val mapper: JsonMapper = jsonMapper { addModule(kotlinModule()) }

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
        } catch (e: JacksonException) {
            throw InvalidRequestException("Can't deserialize data into an object", e.message)
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
        } catch (e: JacksonException) {
            throw InvalidRequestException("Can't deserialize data into list of objects", e.message)
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
        val mapperWithMixin = mapper.rebuild().addMixIn(inputClass.java, filterMixin.java).build()
        val filterProvider = SimpleFilterProvider().addFilter(
            filterMixinName,
            SimpleBeanPropertyFilter.serializeAllExcept(propertiesToExclude)
        )
        return mapperWithMixin.writer(filterProvider).writeValueAsString(input)
    }

    fun Map<String, Any>.getAllKeys(): Set<String> =
        this.entries.fold(emptySet()) { acc, entry ->
            // what is inside the value of a Property or a JsonProperty is not a key
            if (entry.key in COMPACTED_ATTRIBUTES_TERMS)
                acc.plus(entry.key)
            else {
                val valueKeys = when (entry.value) {
                    is Map<*, *> -> (entry.value as Map<String, Any>).getAllKeys()
                    is List<*> ->
                        (entry.value as List<Any>).flatMap {
                            // type value can be a list, not interested in it here
                            if (it is Map<*, *>)
                                (it as Map<String, Any>).getAllKeys()
                            else emptySet()
                        }.toSet()
                    // if it is not a list or an object, it is a value (and thus not a key)
                    else -> emptySet()
                }
                acc.plus(entry.key).plus(valueKeys)
            }
        }

    fun Map<String, Any>.getAllValues(): Set<Any?> =
        this.entries.fold(emptySet()) { acc, entry ->
            val values = when {
                entry.value is Map<*, *> && entry.key in listOf(NGSILD_VALUE_TERM, NGSILD_JSON_TERM) ->
                    setOf(entry.value)
                entry.value is Map<*, *> ->
                    (entry.value as Map<String, Any>).getAllValues()
                entry.value is List<*> ->
                    (entry.value as List<Any>).flatMap {
                        if (it is Map<*, *>) (it as Map<String, Any>).getAllValues()
                        else setOf(it)
                    }.toSet()
                else -> setOf(entry.value)
            }
            acc.plus(values)
        }
}
