package com.egm.datahub.context.search.util

import com.egm.datahub.context.search.model.Entity
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer

class EntitySerializer : StdSerializer<Entity>(Entity::class.java) {

    override fun serialize(entity: Entity, gen: JsonGenerator, provider: SerializerProvider) {

        gen.writeStartObject()

        entity.properties.forEach {
            gen.writeObjectField(it.key, it.value)
        }

        entity.relationships.forEach {
            gen.writeObjectField(it.key, it.value)
        }

        gen.writeEndObject()
    }
}
