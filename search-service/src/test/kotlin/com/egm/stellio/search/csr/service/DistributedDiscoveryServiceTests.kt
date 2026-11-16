package com.egm.stellio.search.csr.service

import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.csr.model.MiscellaneousPersistentWarning
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.NAME_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.notFound
import com.github.tomakehurst.wiremock.client.WireMock.ok
import com.github.tomakehurst.wiremock.client.WireMock.serverError
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.just
import io.mockk.runs
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class DistributedDiscoveryServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var distributedDiscoveryService: DistributedDiscoveryService

    @MockkBean
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

    private val humidityIri = "https://ontology.eglobalmark.com/apic#humidity"

    // Apiary entities with the name attribute, and BeeHive entities with the temperature attribute
    private val csr = gimmeRawCSR(
        information = listOf(
            RegistrationInfo(
                entities = listOf(EntityInfo(types = listOf(APIARY_IRI))),
                propertyNames = listOf(NAME_IRI)
            ),
            RegistrationInfo(
                entities = listOf(EntityInfo(types = listOf(BEEHIVE_IRI))),
                propertyNames = listOf(TEMPERATURE_IRI)
            )
        ),
        operations = listOf(Operation.REDIRECTION_OPS)
    )

    @BeforeEach
    fun mockContextSourceRegistrationService() {
        coEvery { contextSourceRegistrationService.getContextSourceRegistrations(any()) } returns listOf(csr)
        coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } just runs
    }

    @Test
    fun `distributeRetrieveEntityTypes should only return the types registered by the CSR`() = runTest {
        stubFor(
            get(urlPathEqualTo("/ngsi-ld/v1/types"))
                .willReturn(ok().withBody(entityTypeList(APIARY_TERM, BEEHIVE_TERM, "Sensor")))
        )

        val (warnings, results) = distributedDiscoveryService.distributeRetrieveEntityTypes(
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        assertTrue(warnings.isEmpty())
        assertEquals(listOf(APIARY_TERM, BEEHIVE_TERM), results.single().typeList)
        verify(1, getRequestedFor(urlPathEqualTo("/ngsi-ld/v1/types")))
        coVerify {
            contextSourceRegistrationService.getContextSourceRegistrations(
                match { "operations==retrieveEntityTypes" in it.csf.orEmpty() && "redirectionOps" in it.csf.orEmpty() }
            )
            contextSourceRegistrationService.updateContextSourceStatus(csr, true)
        }
    }

    @Test
    fun `distributeRetrieveEntityTypeDetails should only keep the attributes registered for each type`() = runTest {
        stubFor(
            get(urlPathEqualTo("/ngsi-ld/v1/types"))
                .withQueryParam("details", equalTo("true"))
                .willReturn(
                    ok().withBody(
                        """
                        [
                            ${entityType(APIARY_IRI, NAME_TERM, TEMPERATURE_TERM)},
                            ${entityType(BEEHIVE_IRI, NAME_TERM)},
                            ${entityType("https://ontology.eglobalmark.com/egm#Sensor")}
                        ]
                        """.trimIndent()
                    )
                )
        )

        val (warnings, results) = distributedDiscoveryService.distributeRetrieveEntityTypeDetails(
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        assertTrue(warnings.isEmpty())
        // BeeHive is dropped since none of its attributes is registered for it, Sensor since it is not registered
        val entityTypes = results.single()
        assertEquals(1, entityTypes.size)
        assertEquals(APIARY_TERM, entityTypes[0].typeName)
        assertEquals(listOf(NAME_TERM), entityTypes[0].attributeNames)
    }

    @Test
    fun `distributeRetrieveEntityTypeInfo should call the CSR with the encoded type and restrict the attributes`() =
        runTest {
            stubFor(
                get(urlPathEqualTo("/ngsi-ld/v1/types/https:%2F%2Fontology.eglobalmark.com%2Fapic%23Apiary"))
                    .willReturn(
                        ok().withBody(
                            """
                            {
                                "id": "$APIARY_IRI",
                                "type": "EntityTypeInfo",
                                "typeName": "$APIARY_TERM",
                                "entityCount": 12,
                                "attributeDetails": [
                                    ${attributeInfo(NAME_IRI, NAME_TERM)},
                                    ${attributeInfo(TEMPERATURE_IRI, TEMPERATURE_TERM)}
                                ]
                            }
                            """.trimIndent()
                        )
                    )
            )

            val (warnings, results) = distributedDiscoveryService.distributeRetrieveEntityTypeInfo(
                APIARY_IRI,
                HttpHeaders.EMPTY,
                APIC_COMPOUND_CONTEXTS
            )

            assertTrue(warnings.isEmpty())
            val entityTypeInfo = results.single()
            assertEquals(12, entityTypeInfo.entityCount)
            assertEquals(listOf(NAME_TERM), entityTypeInfo.attributeDetails.map { it.attributeName })
        }

    @Test
    fun `distributeRetrieveEntityTypeInfo should ignore a CSR answering with another type`() = runTest {
        stubFor(
            get(urlPathEqualTo("/ngsi-ld/v1/types/https:%2F%2Fontology.eglobalmark.com%2Fapic%23Apiary"))
                .willReturn(
                    ok().withBody(
                        """
                        {
                            "id": "$BEEHIVE_IRI",
                            "type": "EntityTypeInfo",
                            "typeName": "$BEEHIVE_TERM",
                            "entityCount": 1,
                            "attributeDetails": []
                        }
                        """.trimIndent()
                    )
                )
        )

        val (warnings, results) = distributedDiscoveryService.distributeRetrieveEntityTypeInfo(
            APIARY_IRI,
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        assertTrue(warnings.isEmpty())
        assertTrue(results.isEmpty())
    }

    @Test
    fun `distributeRetrieveAttributes should only return the attributes registered by the CSR`() = runTest {
        stubFor(
            get(urlPathEqualTo("/ngsi-ld/v1/attributes"))
                .willReturn(ok().withBody(attributeList(NAME_TERM, TEMPERATURE_TERM, "humidity")))
        )

        val (_, results) = distributedDiscoveryService.distributeRetrieveAttributes(
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        assertEquals(listOf(NAME_TERM, TEMPERATURE_TERM), results.single().attributeList)
    }

    @Test
    fun `distributeRetrieveAttributeDetails should only keep the types registered with each attribute`() = runTest {
        stubFor(
            get(urlPathEqualTo("/ngsi-ld/v1/attributes"))
                .withQueryParam("details", equalTo("true"))
                .willReturn(
                    ok().withBody(
                        """
                        [
                            ${attributeDetails(NAME_IRI, NAME_TERM, APIARY_TERM, BEEHIVE_TERM)},
                            ${attributeDetails(humidityIri, "humidity", APIARY_TERM)}
                        ]
                        """.trimIndent()
                    )
                )
        )

        val (_, results) = distributedDiscoveryService.distributeRetrieveAttributeDetails(
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        val attributes = results.single()
        assertEquals(1, attributes.size)
        assertEquals(setOf(APIARY_TERM), attributes[0].typeNames)
    }

    @Test
    fun `distributeRetrieveAttributeTypeInfo should only keep the types registered with the attribute`() = runTest {
        stubFor(
            get(urlPathEqualTo("/ngsi-ld/v1/attributes/https:%2F%2Fontology.eglobalmark.com%2Fapic%23temperature"))
                .willReturn(
                    ok().withBody(
                        """
                        {
                            "id": "$TEMPERATURE_IRI",
                            "type": "Attribute",
                            "attributeName": "$TEMPERATURE_TERM",
                            "attributeTypes": ["Property"],
                            "typeNames": ["$APIARY_TERM", "$BEEHIVE_TERM"],
                            "attributeCount": 7
                        }
                        """.trimIndent()
                    )
                )
        )

        val (_, results) = distributedDiscoveryService.distributeRetrieveAttributeTypeInfo(
            TEMPERATURE_IRI,
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        val attributeTypeInfo = results.single()
        assertEquals(setOf(BEEHIVE_TERM), attributeTypeInfo.typeNames)
        assertEquals(7, attributeTypeInfo.attributeCount)
    }

    @Test
    fun `distributeRetrieveEntityTypes should ignore a CSR answering with a 404`() = runTest {
        stubFor(get(urlPathEqualTo("/ngsi-ld/v1/types")).willReturn(notFound()))

        val (warnings, results) = distributedDiscoveryService.distributeRetrieveEntityTypes(
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        assertTrue(warnings.isEmpty())
        assertTrue(results.isEmpty())
    }

    @Test
    fun `distributeRetrieveEntityTypes should return a warning when the CSR answers with an error`() = runTest {
        stubFor(get(urlPathEqualTo("/ngsi-ld/v1/types")).willReturn(serverError()))

        val (warnings, results) = distributedDiscoveryService.distributeRetrieveEntityTypes(
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        assertInstanceOf(MiscellaneousPersistentWarning::class.java, warnings.single())
        assertTrue(results.isEmpty())
        coVerify { contextSourceRegistrationService.updateContextSourceStatus(csr, false) }
    }

    @Test
    fun `distributeRetrieveEntityTypes should return a warning when the CSR is unreachable`() = runTest {
        // nothing listens on port 1
        val unreachableCsr = csr.copy(endpoint = "http://localhost:1".toUri())
        coEvery { contextSourceRegistrationService.getContextSourceRegistrations(any()) } returns listOf(unreachableCsr)

        val (warnings, results) = distributedDiscoveryService.distributeRetrieveEntityTypes(
            HttpHeaders.EMPTY,
            APIC_COMPOUND_CONTEXTS
        )

        assertInstanceOf(MiscellaneousWarning::class.java, warnings.single())
        assertTrue(results.isEmpty())
        coVerify { contextSourceRegistrationService.updateContextSourceStatus(unreachableCsr, false) }
    }

    @Test
    fun `distributeRetrieveEntityTypes should return a warning when the CSR answers with a badly formed payload`() =
        runTest {
            stubFor(get(urlPathEqualTo("/ngsi-ld/v1/types")).willReturn(ok().withBody("""{ "typeList": """)))

            val (warnings, results) = distributedDiscoveryService.distributeRetrieveEntityTypes(
                HttpHeaders.EMPTY,
                APIC_COMPOUND_CONTEXTS
            )

            assertInstanceOf(RevalidationFailedWarning::class.java, warnings.single())
            assertTrue(results.isEmpty())
        }

    private fun entityTypeList(vararg types: String): String =
        """
        { "id": "urn:ngsi-ld:EntityTypeList:1", "type": "EntityTypeList", "typeList": ${types.toJsonArray()} }
        """.trimIndent()

    private fun attributeList(vararg attributes: String): String =
        """
        { "id": "urn:ngsi-ld:AttributeList:1", "type": "AttributeList", "attributeList": ${attributes.toJsonArray()} }
        """.trimIndent()

    private fun entityType(id: String, vararg attributeNames: String): String =
        """
        {
            "id": "$id",
            "type": "EntityType",
            "typeName": "${id.substringAfterLast('#')}",
            "attributeNames": ${attributeNames.toJsonArray()}
        }
        """.trimIndent()

    private fun attributeInfo(id: String, name: String): String =
        """{ "id": "$id", "type": "Attribute", "attributeName": "$name", "attributeTypes": ["Property"] }"""

    private fun attributeDetails(id: String, name: String, vararg typeNames: String): String =
        """
        { "id": "$id", "type": "Attribute", "attributeName": "$name", "typeNames": ${typeNames.toJsonArray()} }
        """.trimIndent()

    private fun Array<out String>.toJsonArray(): String = joinToString(",", "[", "]") { "\"$it\"" }
}
