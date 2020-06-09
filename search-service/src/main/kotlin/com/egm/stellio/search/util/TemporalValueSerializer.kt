package com.egm.stellio.search.util

import com.egm.stellio.search.model.TemporalValue
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer

object TemporalValueSerializer : StdSerializer<TemporalValue>(TemporalValue::class.java) {

    override fun serialize(temporalValue: TemporalValue, gen: JsonGenerator, provider: SerializerProvider) {
        gen.writeRawValue("[${temporalValue.value},\"${temporalValue.timestamp}\"]")
    }
}
