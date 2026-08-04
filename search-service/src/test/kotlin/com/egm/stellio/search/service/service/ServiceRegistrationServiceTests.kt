package com.egm.stellio.search.service.service

import com.egm.stellio.search.common.model.UnparsedGeoQuery
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.service.model.InputInformation
import com.egm.stellio.search.service.model.InputInformationType
import com.egm.stellio.search.service.model.ServiceInformation
import com.egm.stellio.search.service.model.ServiceMode
import com.egm.stellio.search.service.model.ServiceRegistration
import com.egm.stellio.search.service.model.ServiceRegistrationFilters
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.DEVICE_IRI
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class ServiceRegistrationServiceTests : WithTimescaleContainer, WithKafkaContainer() {
    @Autowired
    private lateinit var serviceRegistrationService: ServiceRegistrationService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @AfterEach
    fun deleteServiceRegistrations() {
        r2dbcEntityTemplate.delete<ServiceRegistration>()
            .all()
            .block()
    }

    @Test
    fun `create and retrieve should preserve the complete registration`() = runTest {
        val registration = buildRegistration()

        serviceRegistrationService.create(registration).shouldSucceed()

        serviceRegistrationService.getById(registration.id).shouldSucceedWith {
            assertEquals(registration.id, it.id)
            assertEquals(registration.endpoint, it.endpoint)
            assertEquals(ServiceMode.ASYNCHRONOUS, it.mode)
            assertEquals(registration.entities, it.entities)
            assertEquals(registration.serviceInformation, it.serviceInformation)
            assertEquals(registration.q, it.q)
            assertEquals(registration.geoQ, it.geoQ)
            assertEquals(registration.scopeQ, it.scopeQ)
        }
    }

    @Test
    fun `create should reject an existing id`() = runTest {
        val registration = buildRegistration()

        serviceRegistrationService.create(registration).shouldSucceed()
        serviceRegistrationService.create(registration).shouldFail {
            assertInstanceOf<AlreadyExistsException>(it)
        }
    }

    @Test
    fun `discover should match entity id pattern and type`() = runTest {
        val registration = buildRegistration()
        serviceRegistrationService.create(registration).shouldSucceed()

        val matchingFilters = ServiceRegistrationFilters(
            ids = setOf("urn:ngsi-ld:BeeHive:A456".toUri()),
            typeSelection = BEEHIVE_IRI
        )
        val nonMatchingFilters = matchingFilters.copy(typeSelection = DEVICE_IRI)

        assertThat(serviceRegistrationService.getServiceRegistrations(matchingFilters))
            .extracting<String> { it.id.toString() }
            .containsExactly(registration.id.toString())
        assertEquals(
            1,
            serviceRegistrationService.getServiceRegistrationsCount(matchingFilters).getOrNull()
        )
        assertThat(serviceRegistrationService.getServiceRegistrations(nonMatchingFilters)).isEmpty()
    }

    @Test
    fun `discover should require id and type to match the same entity info`() = runTest {
        val requestedId = "urn:ngsi-ld:BeeHive:A456".toUri()
        val registration = buildRegistration().copy(
            entities = listOf(
                EntityInfo(id = requestedId, types = listOf(DEVICE_IRI)),
                EntityInfo(
                    id = "urn:ngsi-ld:BeeHive:B789".toUri(),
                    types = listOf(BEEHIVE_IRI)
                )
            )
        )
        serviceRegistrationService.create(registration).shouldSucceed()

        val filters = ServiceRegistrationFilters(
            ids = setOf(requestedId),
            typeSelection = BEEHIVE_IRI
        )

        assertThat(serviceRegistrationService.getServiceRegistrations(filters)).isEmpty()
        assertEquals(
            0,
            serviceRegistrationService.getServiceRegistrationsCount(filters).getOrNull()
        )
    }

    @Test
    fun `discover should match any registered entity type`() = runTest {
        val registration = buildRegistration().copy(
            id = "urn:ngsi-ld:ServiceRegistration:type-selection".toUri(),
            entities = listOf(
                EntityInfo(
                    id = null,
                    idPattern = null,
                    types = listOf(BEEHIVE_IRI, DEVICE_IRI)
                )
            )
        )
        serviceRegistrationService.create(registration).shouldSucceed()

        listOf(BEEHIVE_IRI, DEVICE_IRI).forEach { entityType ->
            val filters = ServiceRegistrationFilters(
                ids = setOf("urn:ngsi-ld:Entity:A456".toUri()),
                typeSelection = entityType
            )

            assertThat(serviceRegistrationService.getServiceRegistrations(filters))
                .extracting<String> { it.id.toString() }
                .containsExactly(registration.id.toString())
        }
    }

    @Test
    fun `update and delete should complete the lifecycle`() = runTest {
        val registration = buildRegistration()
        serviceRegistrationService.create(registration).shouldSucceed()
        val updated = registration.copy(
            endpoint = "http://localhost:4567/setLight".toUri(),
            modifiedAt = ngsiLdDateTime()
        )

        serviceRegistrationService.upsert(updated).shouldSucceed()
        serviceRegistrationService.getById(registration.id).shouldSucceedWith {
            assertEquals(updated.endpoint, it.endpoint)
        }

        serviceRegistrationService.delete(registration.id).shouldSucceed()
        serviceRegistrationService.getById(registration.id).shouldFail {
            assertInstanceOf<ResourceNotFoundException>(it)
        }
    }

    private fun buildRegistration() =
        ServiceRegistration(
            id = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri(),
            endpoint = "http://localhost:2345/setLight".toUri(),
            mode = ServiceMode.ASYNCHRONOUS,
            entities = listOf(
                EntityInfo(
                    id = null,
                    idPattern = "urn:ngsi-ld:BeeHive:.*",
                    types = listOf(BEEHIVE_IRI)
                )
            ),
            q = "model==\"modelName\"",
            geoQ = UnparsedGeoQuery(
                geometry = "Polygon",
                coordinates = listOf(
                    listOf(
                        listOf(0, 0),
                        listOf(1, 0),
                        listOf(1, 1),
                        listOf(0, 0)
                    )
                ),
                georel = "within"
            ),
            scopeQ = "/building/floor1",
            serviceInformation = ServiceInformation(
                name = "setLight",
                title = "setLight",
                description = "Set brightness of light",
                mode = ServiceMode.ASYNCHRONOUS,
                input = InputInformation(
                    type = InputInformationType.OBJECT,
                    required = true,
                    properties = mapOf(
                        "brightness" to InputInformation(
                            type = InputInformationType.INTEGER,
                            required = true,
                            minimum = 0.toBigDecimal(),
                            maximum = 255.toBigDecimal()
                        )
                    )
                ),
                output = InputInformation(type = InputInformationType.STRING)
            )
        )
}
