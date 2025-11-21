package com.egm.stellio.search.csr.service

import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.ContextSourceRegistration.Companion.notFoundMessage
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.DEVICE_IRI
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.LUMINOSITY_IRI
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class ContextSourceRegistrationServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @AfterEach
    fun deleteContextSourceRegistrations() {
        r2dbcEntityTemplate.delete(ContextSourceRegistration::class.java)
            .all()
            .block()
    }

    fun loadAndDeserializeContextSourceRegistration(
        filename: String,
        contexts: List<String> = APIC_COMPOUND_CONTEXTS
    ): ContextSourceRegistration {
        val csrPayload = loadSampleData(filename)
        return deserializeContextSourceRegistration(csrPayload, contexts)
    }

    fun deserializeContextSourceRegistration(
        csrPayload: String,
        contexts: List<String> = APIC_COMPOUND_CONTEXTS
    ): ContextSourceRegistration =
        ContextSourceRegistration.deserialize(csrPayload.deserializeAsMap(), contexts)
            .shouldSucceedAndResult()

    @Test
    fun `create a second CSR with the same id should return an AlreadyExist error`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.create(contextSourceRegistration).shouldSucceed()
        contextSourceRegistrationService.create(contextSourceRegistration).shouldFailWith {
            it is AlreadyExistsException
        }
    }

    @Test
    fun `get a minimal CSR should return the created CSR`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.create(contextSourceRegistration).shouldSucceed()

        contextSourceRegistrationService.getById(
            contextSourceRegistration.id
        ).shouldSucceedWith {
            assertEquals(contextSourceRegistration, it)
        }
    }

    @Test
    fun `get a full CSR should return the created CSR`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_full.json")
        contextSourceRegistrationService.create(contextSourceRegistration).shouldSucceed()

        contextSourceRegistrationService.getById(
            contextSourceRegistration.id
        ).shouldSucceedWith {
            assertEquals(contextSourceRegistration, it)
        }
    }

    @Test
    fun `query CSR on entities ids should return a CSR matching this id uniquely`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.create(contextSourceRegistration).shouldSucceed()

        val matchingCsrs = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(ids = setOf("urn:ngsi-ld:BeeHive:A456".toUri()))
        )

        assertEquals(1, matchingCsrs.size)
    }

    @Test
    fun `query CSR on entities ids should return a CSR matching this id in one of the entities`() = runTest {
        val contextSourceRegistration =
            deserializeContextSourceRegistration(
                """
                {
                  "id": "urn:ngsi-ld:ContextSourceRegistration:1",
                  "type": "ContextSourceRegistration",
                  "information": [
                    {
                      "entities": [
                        {
                          "id": "urn:ngsi-ld:Vehicle:A456",
                          "type": "Vehicle"
                        },
                        {
                          "id": "urn:ngsi-ld:Vehicle:A457",
                          "type": "Vehicle"
                        }
                      ]
                    }
                  ],
                  "endpoint": "http://my.csr.endpoint/"
                }                    
                """.trimIndent()
            )
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        val matchingCsrs = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(ids = setOf("urn:ngsi-ld:Vehicle:A456".toUri()))
        )

        assertEquals(1, matchingCsrs.size)
    }

    @Test
    fun `query CSR on entities ids should return a CSR matching this id on idPattern`() = runTest {
        val contextSourceRegistration =
            deserializeContextSourceRegistration(
                """
                {
                  "id": "urn:ngsi-ld:ContextSourceRegistration:1",
                  "type": "ContextSourceRegistration",
                  "information": [
                    {
                      "entities": [
                        {
                          "idPattern": "urn:ngsi-ld:Vehicle:A4*",
                          "type": "Vehicle"
                        }
                      ]
                    }
                  ],
                  "endpoint": "http://my.csr.endpoint/"
                }                    
                """.trimIndent()
            )
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        val matchingCsrs = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(ids = setOf("urn:ngsi-ld:Vehicle:A456".toUri()))
        )

        assertEquals(1, matchingCsrs.size)
    }

    @Test
    fun `query CSR on entities ids should return a CSR matching this id on idPattern but not on id`() = runTest {
        val contextSourceRegistration =
            deserializeContextSourceRegistration(
                """
                {
                  "id": "urn:ngsi-ld:ContextSourceRegistration:1",
                  "type": "ContextSourceRegistration",
                  "information": [
                    {
                      "entities": [
                        {
                          "idPattern": "urn:ngsi-ld:Vehicle:A4*",
                          "type": "Vehicle"
                        },
                        {
                          "id": "urn:ngsi-ld:Vehicle:B123",
                          "type": "Vehicle"
                        }
                      ]
                    }
                  ],
                  "endpoint": "http://my.csr.endpoint/"
                }                    
                """.trimIndent()
            )
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        val matchingCsrs = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(ids = setOf("urn:ngsi-ld:Vehicle:A456".toUri()))
        )

        assertEquals(1, matchingCsrs.size)
    }

    @Test
    fun `query CSR on entities ids should return a single CSR when entities matches twice`() = runTest {
        val contextSourceRegistration =
            deserializeContextSourceRegistration(
                """
                {
                  "id": "urn:ngsi-ld:ContextSourceRegistration:1",
                  "type": "ContextSourceRegistration",
                  "information": [
                    {
                      "entities": [
                        {
                          "idPattern": "urn:ngsi-ld:Vehicle:A4*",
                          "type": "Vehicle"
                        },
                        {
                          "id": "urn:ngsi-ld:Vehicle:A456",
                          "type": "Vehicle"
                        }
                      ]
                    }
                  ],
                  "endpoint": "http://my.csr.endpoint/"
                }                    
                """.trimIndent()
            )
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        val matchingCsrs = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(ids = setOf("urn:ngsi-ld:Vehicle:A456".toUri()))
        )

        assertEquals(1, matchingCsrs.size)
    }

    @Test
    fun `query CSR on entities ids should return an empty list if no CSR matches`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        val matchingCsrs = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(ids = setOf("urn:ngsi-ld:Vehicle:A457".toUri()))
        )

        assertTrue(matchingCsrs.isEmpty())
    }

    @Test
    fun `query on CSR operations should filter the result`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        val oneMatchingOperationCsrs = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(operations = listOf(Operation.FEDERATION_OPS))
        )
        assertEquals(1, oneMatchingOperationCsrs.size)

        val twoOperationOneMatchingCsrs = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(operations = listOf(Operation.FEDERATION_OPS, Operation.CREATE_ENTITY))
        )
        assertEquals(1, twoOperationOneMatchingCsrs.size)

        val notMatchingCsr = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(operations = listOf(Operation.CREATE_ENTITY))
        )
        assertTrue(notMatchingCsr.isEmpty())
    }

    @Test
    fun `query on CSR entity types should filter the result`() = runTest {
        val csr =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.upsert(csr).shouldSucceed()
        val oneCsrMatching = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(typeSelection = BEEHIVE_IRI)
        )
        assertEquals(listOf(csr), oneCsrMatching)

        val multipleTypesOneCsrMatching = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(typeSelection = "$BEEHIVE_IRI|$DEVICE_IRI")
        )
        assertEquals(listOf(csr), multipleTypesOneCsrMatching)

        val notMatchingCsr = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(typeSelection = "INVALID")
        )
        assertTrue(notMatchingCsr.isEmpty())
    }

    @Test
    fun `query on CSR entity idPattern should filter the result`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        val oneCsrMatching = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(idPattern = ".*")
        )
        assertEquals(listOf(contextSourceRegistration), oneCsrMatching)

        val notMatchingCsr = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(idPattern = "INVALID")
        )
        assertTrue(notMatchingCsr.isEmpty())
    }

    @Test
    fun `query on CSR information attributes should filter the result`() = runTest {
        val newCsr =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
                .copy(
                    information = listOf(
                        RegistrationInfo(null, listOf(TEMPERATURE_IRI), null)
                    )
                )

        contextSourceRegistrationService.upsert(newCsr).shouldSucceed()

        val oneCsrMatching = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(attrs = setOf(TEMPERATURE_IRI))
        )
        assertEquals(listOf(newCsr), oneCsrMatching)

        val notMatchingCsr = contextSourceRegistrationService.getContextSourceRegistrations(
            CSRFilters(attrs = setOf(LUMINOSITY_IRI))
        )
        assertThat(notMatchingCsr).isEmpty()
    }

    @Test
    fun `count should apply the filter`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        val count = contextSourceRegistrationService.getContextSourceRegistrationsCount(
            CSRFilters(idPattern = ".*")
        )
        assertEquals(1, count.getOrNull())

        val countEmpty = contextSourceRegistrationService.getContextSourceRegistrationsCount(
            CSRFilters(idPattern = "INVALID")
        )
        assertEquals(0, countEmpty.getOrNull())
    }

    @Test
    fun `delete an existing CSR should succeed`() = runTest {
        val contextSourceRegistration =
            loadAndDeserializeContextSourceRegistration("csr/contextSourceRegistration_minimal_entities.json")
        contextSourceRegistrationService.upsert(contextSourceRegistration).shouldSucceed()

        contextSourceRegistrationService.delete(contextSourceRegistration.id).shouldSucceed()

        contextSourceRegistrationService.getById(contextSourceRegistration.id).shouldFailWith {
            it is ResourceNotFoundException &&
                it.message == notFoundMessage(contextSourceRegistration.id)
        }
    }

    @Test
    fun `delete a non existing CSR should return a RessourceNotFound error`() = runTest {
        val id = "urn:ngsi-ld:ContextSourceRegistration:UnknownContextSourceRegistration".toUri()
        contextSourceRegistrationService.delete(
            id
        ).shouldFailWith {
            it is ResourceNotFoundException &&
                it.message == notFoundMessage(id)
        }
    }
}
