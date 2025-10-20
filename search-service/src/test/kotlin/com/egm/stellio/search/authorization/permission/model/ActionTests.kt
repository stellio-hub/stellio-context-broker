package com.egm.stellio.search.authorization.permission.model

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.toNgsiLdAttribute
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ActionTests {
    private val authContexts = listOf(
        "http://localhost:8093/jsonld-contexts/authorization.jsonld",
        "http://localhost:8093/jsonld-contexts/ngsi-ld-core-context-v1.8.jsonld"
    )

    @Test
    fun `it should return a 400 if the payload contains a multi-instance property`() = runTest {
        val requestPayload =
            """
            [{
                "type": "Property",
                "value": "read"
            },{
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Dataset:01",
                "value": "write"
            }]
            """.trimIndent()

        val ngsiLdAttribute =
            expandAttribute(AUTH_TERM_SAP, requestPayload, authContexts)
                .toNgsiLdAttribute()
                .shouldSucceedAndResult()

        ngsiLdAttribute.getSpecificAccessPolicy()
            .shouldFail {
                assertThat(it)
                    .isInstanceOf(BadRequestDataException::class.java)
                    .hasFieldOrPropertyWithValue("message", "Payload must contain a single attribute instance")
            }
    }

    @Test
    fun `it should ignore properties that are not part of the payload when setting a specific access policy`() =
        runTest {
            val requestPayload =
                """
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "value": "read"
                }
                """.trimIndent()

            val ngsiLdAttribute =
                expandAttribute(AUTH_TERM_SAP, requestPayload, authContexts)
                    .toNgsiLdAttribute()
                    .shouldSucceedAndResult()

            ngsiLdAttribute.getSpecificAccessPolicy()
                .shouldSucceedWith { assertEquals(Action.READ, it) }
        }

    @Test
    fun `it should accept AUTH_READ as a specific access policy`() =
        runTest {
            val requestPayload =
                """
                {
                    "type": "Property",
                    "value": "AUTH_READ"
                }
                """.trimIndent()

            val ngsiLdAttribute =
                expandAttribute(AUTH_TERM_SAP, requestPayload, authContexts)
                    .toNgsiLdAttribute()
                    .shouldSucceedAndResult()

            ngsiLdAttribute.getSpecificAccessPolicy()
                .shouldSucceedWith { assertEquals(Action.READ, it) }
        }

    @Test
    fun `it should return a 400 if the value is not one of the supported`() = runTest {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "someValue"
            }
            """.trimIndent()

        val ngsiLdAttribute =
            expandAttribute(AUTH_TERM_SAP, requestPayload, authContexts)
                .toNgsiLdAttribute()
                .shouldSucceedAndResult()

        val expectedMessage =
            """Invalid action provided: "someValue", must be "own", "admin","write" or "read"."""

        ngsiLdAttribute.getSpecificAccessPolicy()
            .shouldFail {
                assertThat(it)
                    .isInstanceOf(BadRequestDataException::class.java)
                    .hasFieldOrPropertyWithValue("message", expectedMessage)
            }
    }

    @Test
    fun `it should return a 400 if the provided attribute is a relationship`() = runTest {
        val requestPayload =
            """
            {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Entity:01"
            }
            """.trimIndent()

        val ngsiLdAttribute =
            expandAttribute(AUTH_TERM_SAP, requestPayload, authContexts)
                .toNgsiLdAttribute()
                .shouldSucceedAndResult()

        ngsiLdAttribute.getSpecificAccessPolicy()
            .shouldFail {
                assertThat(it)
                    .isInstanceOf(BadRequestDataException::class.java)
                    .hasFieldOrPropertyWithValue("message", "Payload must be a property")
            }
    }
}
