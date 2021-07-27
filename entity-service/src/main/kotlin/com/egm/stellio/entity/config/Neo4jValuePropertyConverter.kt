package com.egm.stellio.entity.config

import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.ListValue
import org.neo4j.driver.internal.value.StringValue
import org.springframework.data.neo4j.core.convert.Neo4jPersistentPropertyConverter
import java.net.URI

const val jsonObjectPrefix = "jsonObject@"

class Neo4jValuePropertyConverter : Neo4jPersistentPropertyConverter<Any> {

    @Suppress("SpreadOperator")
    override fun write(source: Any): Value {
        return when (source) {
            is List<*> -> ListValue(*source.map { Values.value(it) }.toTypedArray())
            is URI -> StringValue(source.toString())
            is Map<*, *> -> {
                // there is no neo4j support for JSON object
                // store the serialized map prefixed with 'jsonObject@' to know how to deserialize it later
                val value = jsonObjectPrefix + serializeObject(source)
                StringValue(value)
            }
            else -> Values.value(source)
        }
    }

    override fun read(source: Value): Any {
        return when (source) {
            is ListValue -> source.asList()
            is StringValue -> {
                if (source.asString().startsWith(jsonObjectPrefix)) {
                    val sourceString = source.asString().removePrefix(jsonObjectPrefix)
                    deserializeAs<Map<String, Any>>(sourceString)
                } else Values.ofObject().apply(source)
            }
            else -> Values.ofObject().apply(source)
        }
    }
}
