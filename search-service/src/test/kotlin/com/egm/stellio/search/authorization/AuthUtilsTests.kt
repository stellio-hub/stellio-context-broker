package com.egm.stellio.search.authorization

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.toNgsiLdAttribute
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class AuthUtilsTests {

    private val applicationProperties = mockk<ApplicationProperties> {
        every {
            contexts.defaultAuthzContexts()
        } returns listOf(
            "http://localhost:8093/jsonld-contexts/authorization.jsonld",
            "http://localhost:8093/jsonld-contexts/ngsi-ld-core-context-v1.8.jsonld"
        )
    }

    @Test
    fun `it should return a 400 if the payload contains a multi-instance property`() = runTest {
        val requestPayload =
            """
            [{
                "type": "Property",
                "value": "AUTH_READ"
            },{
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Dataset:01",
                "value": "AUTH_WRITE"
            }]
            """.trimIndent()

        val ngsiLdAttribute =
            expandAttribute(AUTH_TERM_SAP, requestPayload, applicationProperties.contexts.defaultAuthzContexts())
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
                    "value": "AUTH_READ"
                }
                """.trimIndent()

            val ngsiLdAttribute =
                expandAttribute(AUTH_TERM_SAP, requestPayload, applicationProperties.contexts.defaultAuthzContexts())
                    .toNgsiLdAttribute()
                    .shouldSucceedAndResult()

            ngsiLdAttribute.getSpecificAccessPolicy()
                .shouldSucceedWith { Assertions.assertEquals(AuthContextModel.SpecificAccessPolicy.AUTH_READ, it) }
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
            expandAttribute(AUTH_TERM_SAP, requestPayload, applicationProperties.contexts.defaultAuthzContexts())
                .toNgsiLdAttribute()
                .shouldSucceedAndResult()

        val expectedMessage =
            "Value must be one of AUTH_READ or AUTH_WRITE " +
                "(No enum constant com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.someValue)"

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
            expandAttribute(AUTH_TERM_SAP, requestPayload, applicationProperties.contexts.defaultAuthzContexts())
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
