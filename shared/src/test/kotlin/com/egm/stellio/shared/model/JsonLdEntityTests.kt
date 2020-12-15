package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.matchContent
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType

class JsonLdEntityTests {
    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    @Test
    fun `it should compact and return a JSON entity`() {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()

        val jsonLdEntity = expandJsonLdEntity(entity, DEFAULT_CONTEXTS)
        val compactedEntity = jsonLdEntity.compact(MediaType.APPLICATION_JSON)

        assertTrue(mapper.writeValueAsString(compactedEntity).matchContent(entity))
    }

    @Test
    fun `it should compact and return a JSON-LD entity`() {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()
        val expectedEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "@context":[
                    "https://fiware.github.io/data-models/context.jsonld",
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
                ]
            }
            """.trimIndent()

        val jsonLdEntity = expandJsonLdEntity(entity, DEFAULT_CONTEXTS)
        val compactedEntity = jsonLdEntity.compact()

        assertTrue(mapper.writeValueAsString(compactedEntity).matchContent(expectedEntity))
    }
}
