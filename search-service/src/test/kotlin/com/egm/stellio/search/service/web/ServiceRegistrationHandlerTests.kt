package com.egm.stellio.search.service.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.common.model.UnparsedGeoQuery
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.service.model.ServiceInformation
import com.egm.stellio.search.service.model.ServiceRegistration
import com.egm.stellio.search.service.service.ServiceRegistrationService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
@AutoConfigureWebTestClient(timeout = "30000")
@WebFluxTest(ServiceRegistrationHandler::class)
class ServiceRegistrationHandlerTests {
    @MockkBean
    private lateinit var serviceRegistrationService: ServiceRegistrationService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @Autowired
    private lateinit var webClient: WebTestClient

    private val resourceUri = "/ngsi-ld/v1/serviceRegistrations"
    private val registrationId = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri()
    private val entityId = "urn:ngsi-ld:BeeHive:A456".toUri()
    private val registration = ServiceRegistration(
        id = registrationId,
        endpoint = "http://localhost:2345/setLight".toUri(),
        entities = listOf(
            EntityInfo(
                id = null,
                idPattern = "urn:ngsi-ld:BeeHive:.*",
                types = listOf(BEEHIVE_IRI)
            )
        ),
        geoQ = UnparsedGeoQuery(
            geometry = "Point",
            coordinates = listOf(0, 0),
            georel = "within"
        ),
        scopeQ = "/building/floor1",
        serviceInformation = ServiceInformation(name = "setLight")
    )

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, APIC_HEADER_LINK)
            }
            .build()
    }

    @BeforeEach
    fun allowAdminAccess() {
        coEvery { authorizationService.userIsAdmin() } returns Unit.right()
    }

    @Test
    fun `create should return 201 and preserve the service query criteria`() = runTest {
        coEvery { serviceRegistrationService.create(any()) } returns Unit.right()

        webClient.post()
            .uri(resourceUri)
            .bodyValue(serviceRegistrationPayload)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().valueEquals("Location", "$resourceUri/$registrationId")

        coVerify {
            serviceRegistrationService.create(
                match {
                    it.id == registrationId &&
                        it.endpoint.toString() == "http://localhost:2345/setLight" &&
                        it.serviceInformation.name == "setLight" &&
                        it.scopeQ == "/building/floor1" &&
                        it.geoQ?.geometry == "Point"
                }
            )
        }
    }

    @Test
    fun `query should return matching registrations and count`() = runTest {
        coEvery {
            serviceRegistrationService.getServiceRegistrations(any(), any(), any())
        } returns listOf(registration)
        coEvery { serviceRegistrationService.getServiceRegistrationsCount(any()) } returns 1.right()

        webClient.get()
            .uri { uriBuilder ->
                uriBuilder.path(resourceUri)
                    .queryParam("id", entityId)
                    .queryParam("type", "BeeHive")
                    .queryParam("count", "true")
                    .build()
            }
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "1")
            .expectBody()
            .jsonPath("$[0].id").isEqualTo(registrationId.toString())
            .jsonPath("$[0].serviceInformation.name").isEqualTo("setLight")

        coVerify {
            serviceRegistrationService.getServiceRegistrations(
                match {
                    it.ids == setOf(entityId) &&
                        it.typeSelection == BEEHIVE_IRI
                },
                any(),
                any()
            )
        }
    }

    @Test
    fun `query should reject a request without both id and type`() = runTest {
        webClient.get()
            .uri("$resourceUri?id=$entityId")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody()
            .jsonPath("$.title")
            .isEqualTo("Parameters 'id' and 'type' are required to discover service registrations")
    }

    @Test
    fun `query should reject scope and geo query parameters`() = runTest {
        listOf(
            "$resourceUri?id=$entityId&type=BeeHive&scopeQ=/building/%23",
            "$resourceUri?id=$entityId&type=BeeHive&geometry=Point&coordinates=%5B0%2C0%5D&georel=within"
        ).forEach { uri ->
            webClient.get()
                .uri(uri)
                .exchange()
                .expectStatus().isBadRequest
        }
    }

    @Test
    fun `retrieve should return the registration without system attributes by default`() = runTest {
        coEvery { serviceRegistrationService.getById(registrationId) } returns registration.right()

        webClient.get()
            .uri("$resourceUri/$registrationId")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isEqualTo(registrationId.toString())
            .jsonPath("$.createdAt").doesNotExist()
            .jsonPath("$.modifiedAt").doesNotExist()
    }

    @Test
    fun `retrieve should propagate a not found error`() = runTest {
        coEvery {
            serviceRegistrationService.getById(registrationId)
        } returns ResourceNotFoundException("not found").left()

        webClient.get()
            .uri("$resourceUri/$registrationId")
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    fun `update should merge the fragment and return 204`() = runTest {
        coEvery { serviceRegistrationService.getById(registrationId) } returns registration.right()
        coEvery { serviceRegistrationService.upsert(any()) } returns Unit.right()

        webClient.patch()
            .uri("$resourceUri/$registrationId")
            .bodyValue("""{"endpoint":"http://localhost:4567/setLight"}""")
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            serviceRegistrationService.upsert(
                match {
                    it.id == registrationId &&
                        it.endpoint.toString() == "http://localhost:4567/setLight"
                }
            )
        }
    }

    @Test
    fun `delete should return 204`() = runTest {
        coEvery { serviceRegistrationService.getById(registrationId) } returns registration.right()
        coEvery { serviceRegistrationService.delete(registrationId) } returns Unit.right()

        webClient.delete()
            .uri("$resourceUri/$registrationId")
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            serviceRegistrationService.delete(registrationId)
        }
    }

    private val serviceRegistrationPayload =
        """
        {
          "id": "$registrationId",
          "type": "ServiceRegistration",
          "endpoint": "http://localhost:2345/setLight",
          "entities": [{
            "idPattern": "urn:ngsi-ld:BeeHive:.*",
            "type": "BeeHive"
          }],
          "geoQ": {
            "geometry": "Point",
            "coordinates": [0, 0],
            "georel": "within"
          },
          "scopeQ": "/building/floor1",
          "serviceInformation": {
            "name": "setLight",
            "mode": "asynchronous",
            "input": {
              "type": "object"
            }
          }
        }
        """.trimIndent()
}
