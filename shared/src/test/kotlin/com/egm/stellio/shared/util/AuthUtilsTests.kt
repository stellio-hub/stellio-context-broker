package com.egm.stellio.shared.util

import arrow.core.None
import arrow.core.Some
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.toNgsiLdAttribute
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SID
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@ActiveProfiles("test")
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
    fun `it should extract sub from an entity URI`() {
        assertEquals(
            "3693C62A-D5B2-4F9E-9D3A-F82814984D5C",
            URI.create("urn:ngsi-ld:Entity:3693C62A-D5B2-4F9E-9D3A-F82814984D5C").extractSub()
        )
    }

    @Test
    fun `it should extract sub from a string version of an entity URI`() {
        assertEquals(
            "3693C62A-D5B2-4F9E-9D3A-F82814984D5C",
            "urn:ngsi-ld:Entity:3693C62A-D5B2-4F9E-9D3A-F82814984D5C".extractSub()
        )
    }

    @Test
    fun `it should extract the compact form of an authorization term`() {
        assertEquals("serviceAccountId", AUTH_TERM_SID.toCompactTerm())
    }

    @Test
    fun `it should find the global role with a given key`() {
        assertEquals(Some(GlobalRole.STELLIO_ADMIN), GlobalRole.forKey("stellio-admin"))
        assertEquals(Some(GlobalRole.STELLIO_CREATOR), GlobalRole.forKey("stellio-creator"))
    }

    @Test
    fun `it should not find the global role for an unknown key`() {
        assertEquals(None, GlobalRole.forKey("unknown-role"))
    }

    @Test
    fun `it should find the access right with a given key`() {
        assertEquals(Some(AccessRight.CAN_READ), AccessRight.forAttributeName("canRead"))
        assertEquals(Some(AccessRight.CAN_WRITE), AccessRight.forAttributeName("canWrite"))
        assertEquals(Some(AccessRight.CAN_ADMIN), AccessRight.forAttributeName("canAdmin"))
    }

    @Test
    fun `it should not find the access right for an unknown key`() {
        assertEquals(None, AccessRight.forAttributeName("unknown-access-right"))
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
                .shouldSucceedWith { assertEquals(AuthContextModel.SpecificAccessPolicy.AUTH_READ, it) }
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
