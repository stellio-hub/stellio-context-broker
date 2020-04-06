package com.egm.stellio.search.util

import com.egm.stellio.search.model.RawValue
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer

object RawValueSerializer : StdSerializer<RawValue>(RawValue::class.java) {

    override fun serialize(rawValue: RawValue, gen: JsonGenerator, provider: SerializerProvider) {
        gen.writeRawValue("[\"${rawValue.value}\",\"${rawValue.timestamp}\"]")
    }
}
