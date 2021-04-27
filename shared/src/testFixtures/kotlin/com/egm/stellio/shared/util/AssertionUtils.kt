package com.egm.stellio.shared.util

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals

val mapper: ObjectMapper =
    jacksonObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

fun assertJsonPayloadsAreEqual(expectation: String, actual: String) =
    assertEquals(mapper.readTree(expectation), mapper.readTree(actual))
