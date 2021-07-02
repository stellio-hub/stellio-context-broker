package com.egm.stellio.entity.config

import com.egm.stellio.shared.util.toUri
import org.neo4j.driver.Value
import org.neo4j.driver.internal.value.StringValue
import org.springframework.data.neo4j.core.convert.Neo4jPersistentPropertyConverter
import java.net.URI

class Neo4jUriPropertyConverter : Neo4jPersistentPropertyConverter<URI> {

    override fun write(source: URI): Value {
        return StringValue(source.toString())
    }

    override fun read(source: Value): URI {
        return source.asString().toUri()
    }
}
