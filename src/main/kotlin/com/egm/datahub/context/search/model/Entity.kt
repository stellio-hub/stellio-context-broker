package com.egm.datahub.context.search.model

import com.egm.datahub.context.search.util.EntitySerializer
import com.fasterxml.jackson.databind.annotation.JsonSerialize

@JsonSerialize(using = EntitySerializer::class)
class Entity {
    val properties = mutableMapOf<String, Any>()
    val relationships = mutableMapOf<String, Any>()
    val contexts = mutableListOf<String>()

    fun addProperty(name: String, value: Any) = properties.put(name, value)
    fun addRelationship(name: String, value: Any) = relationships.put(name, value)
    fun addContexts(ctxs: List<String>) = contexts.addAll(ctxs)

    fun addTemporalValues(temporalProperty: String, values: List<Map<String, Any>>) {
        val simplifiedValues = values.map { listOf(it["VALUE"], it["OBSERVED_AT"]) }
        if (getPropertyValue(temporalProperty) == null) {
            addProperty(temporalProperty, mapOf("values" to simplifiedValues))
        } else {
            val targetProperty = getPropertyValue(temporalProperty) as MutableMap<String, Any>
            targetProperty["values"] = simplifiedValues
        }
    }

    fun getPropertyValue(name: String): Any? = properties[name]
    fun getRelationshipValue(name: String): Map<String, Any>? = relationships[name] as Map<String, Any>
}
