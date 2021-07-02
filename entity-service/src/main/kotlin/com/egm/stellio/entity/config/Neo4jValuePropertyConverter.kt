package com.egm.stellio.entity.config

import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.ListValue
import org.neo4j.driver.internal.value.StringValue
import org.springframework.data.neo4j.core.convert.Neo4jPersistentPropertyConverter
import java.net.URI

class Neo4jValuePropertyConverter : Neo4jPersistentPropertyConverter<Any> {

    @Suppress("SpreadOperator")
    override fun write(source: Any): Value {
        return when (source) {
            is List<*> -> ListValue(*source.map { Values.value(it) }.toTypedArray())
            is URI -> StringValue(source.toString())
            else -> Values.value(source)
        }
    }

    override fun read(source: Value): Any {
        return when (source) {
            is ListValue -> source.asList()
            else -> Values.ofObject().apply(source)
        }
    }
}
