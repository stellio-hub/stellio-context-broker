package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class JsonLdUtilsTests {

    @Test
    fun `it should throw a LdContextNotAvailable exception if the provided JSON-LD context is not available`() =
        runTest {
            val rawEntity =
                """
                {
                    "id": "urn:ngsi-ld:Device:01234",
                    "type": "Device",
                    "@context": [
                        "unknownContext"
                    ]
                }
                """.trimIndent()

            val exception = assertThrows<LdContextNotAvailableException> {
                expandJsonLdFragment(rawEntity.deserializeAsMap(), listOf("unknownContext"))
            }
            assertEquals(
                "Unable to load remote context (cause was: JsonLdError[code=There was a problem encountered " +
                    "loading a remote context [code=LOADING_REMOTE_CONTEXT_FAILED]., " +
                    "message=Context URI is not absolute [unknownContext].])",
                exception.message
            )
        }

    @Test
    fun `it should throw a BadRequestData exception if the expanded JSON-LD fragment is empty`() = runTest {
        val rawEntity =
            """
            {
                "@context": [
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            expandJsonLdFragment(rawEntity, listOf(NGSILD_TEST_CORE_CONTEXT))
        }
        assertEquals(
            "Unable to expand input payload",
            exception.message
        )
    }

    @Test
    fun `it should compact and return a JSON entity`() = runTest {
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
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "@context": "$NGSILD_TEST_CORE_CONTEXT"
            }
            """.trimIndent()

        val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(entity, NGSILD_TEST_CORE_CONTEXTS)
        val compactedEntity = compactEntity(jsonLdEntity, NGSILD_TEST_CORE_CONTEXTS)

        assertJsonPayloadsAreEqual(expectedEntity, mapper.writeValueAsString(compactedEntity))
    }
}
