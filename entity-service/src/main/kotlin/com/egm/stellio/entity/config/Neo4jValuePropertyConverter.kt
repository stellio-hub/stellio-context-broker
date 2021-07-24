package com.egm.stellio.entity.config

import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.ListValue
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.driver.internal.value.StringValue
import org.springframework.core.convert.support.DefaultConversionService
import org.springframework.data.neo4j.core.convert.Neo4jPersistentPropertyConverter
import org.springframework.util.ReflectionUtils
import java.net.URI

class Neo4jValuePropertyConverter : Neo4jPersistentPropertyConverter<Any> {

    private val conversionService = DefaultConversionService()

    @Suppress("SpreadOperator")
    override fun write(source: Any): Value {
        return when (source) {
            is List<*> -> ListValue(*source.map { Values.value(it) }.toTypedArray())
            is URI -> StringValue(source.toString())
            is Map<*, *> -> convertMap(source)
            else -> Values.value(source)
        }
    }

    override fun read(source: Value): Any {
        return when (source) {
            is ListValue -> source.asList()
            else -> Values.ofObject().apply(source)
        }
    }

    private fun convertObject(input: Any?): Value {
        return when (input) {
            null -> Values.NULL
            is Collection<*> -> convertCollection(input)
            is Array<*> -> convertCollection(input.toList())
            is Map<*, *> -> convertMap(input)
            else -> Values.value(input)
            // {
            //     if (conversionService.canConvert(input::class.java, Value::class.java)) {
            //         conversionService.convert(input, Value::class.java)!!
            //     } else {
            //         convertObjectToMapOfValues(input)
            //     }
            // }
        }
    }

    private fun convertObjectToMapOfValues(input: Any?): Value {
        if (input == null) return Values.NULL

        val output = HashMap<String, Value>()

        ReflectionUtils.doWithFields(input::class.java) {
            ReflectionUtils.makeAccessible(it)
            val fieldValue = ReflectionUtils.getField(it, input)
            output[it.name] = convertObject(fieldValue)
        }

        return MapValue(output)
    }

    private fun convertCollection(input: Collection<*>): ListValue {
        return ListValue(*input.map { convertObject(it) }.toTypedArray())
    }

    private fun convertMap(input: Map<*, *>): MapValue {
        return MapValue(input.map { it.key.toString() to convertObject(it.value) }.toMap())
    }
}
