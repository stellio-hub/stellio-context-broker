package com.egm.stellio.search.csr.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.service.ContextSourceRegistrationService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.AQUAC_HEADER_LINK
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
@AutoConfigureWebTestClient(timeout = "30000")
@WebFluxTest(ContextSourceRegistrationHandler::class)
class ContextSourceRegistrationHandlerTests {

    @MockkBean
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @Autowired
    private lateinit var webClient: WebTestClient

    private val csrUri = "/ngsi-ld/v1/csourceRegistrations"
    private val id = "urn:ngsi-ld:ContextSourceRegistration:1".toUri()
    private val endpoint = "http://my.csr.endpoint/".toUri()

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @BeforeEach
    fun setIsAdmin() {
        coEvery { authorizationService.userIsAdmin() } returns Unit.right()
    }

    @Test
    fun `get CSR by id should return 200 when it exists`() = runTest {
        val contextSourceRegistration = ContextSourceRegistration(id = id, endpoint = endpoint)

        coEvery { contextSourceRegistrationService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { contextSourceRegistrationService.getById(any()) } returns contextSourceRegistration.right()

        webClient.get()
            .uri("$csrUri/$id")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").doesNotExist()
            .jsonPath("$..modifiedAt").doesNotExist()

        coVerify { contextSourceRegistrationService.getById(id) }
    }

    @Test
    fun `get CSR by id should return 200 with sysAttrs when options query param specify it`() = runTest {
        val contextSourceRegistration = ContextSourceRegistration(id = id, endpoint = endpoint)

        coEvery { contextSourceRegistrationService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { contextSourceRegistrationService.getById(any()) } returns contextSourceRegistration.right()

        webClient.get()
            .uri("$csrUri/$id?options=sysAttrs")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").exists()

        coVerify { contextSourceRegistrationService.getById(id) }
    }

    @Test
    fun `get CSR by id should return the errors from the service`() = runTest {
        val contextSourceRegistration = ContextSourceRegistration(id = id, endpoint = endpoint)

        coEvery { contextSourceRegistrationService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { contextSourceRegistrationService.getById(any()) } returns ResourceNotFoundException("").left()

        webClient.get()
            .uri("$csrUri/$id")
            .exchange()
            .expectStatus().isNotFound

        coVerify { contextSourceRegistrationService.getById(contextSourceRegistration.id) }
    }

    @Test
    fun `query CSR should return 200 whether a CSR exists or not`() = runTest {
        val contextSourceRegistration = ContextSourceRegistration(id = id, endpoint = endpoint)

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(contextSourceRegistration)

        coEvery { contextSourceRegistrationService.getContextSourceRegistrationsCount(any()) } returns 1.right()

        webClient.get()
            .uri("$csrUri?id=$id")
            .exchange()
            .expectStatus().isOk
            .expectBody()

        coVerify { contextSourceRegistrationService.getContextSourceRegistrations(any(), any()) }
    }

    @Test
    fun `query CSR should return the count if it was asked`() = runTest {
        val contextSourceRegistration = ContextSourceRegistration(id = id, endpoint = endpoint)

        coEvery { contextSourceRegistrationService.isCreatorOf(any(), any()) } returns true.right()
        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(contextSourceRegistration)

        coEvery { contextSourceRegistrationService.getContextSourceRegistrationsCount(any()) } returns 1.right()

        webClient.get()
            .uri("$csrUri?id=$id&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().exists(RESULTS_COUNT_HEADER)
            .expectBody()

        coVerify { contextSourceRegistrationService.getContextSourceRegistrations(any(), any()) }
    }

    @Test
    fun `delete CSR should return the errors from the service`() = runTest {
        coEvery { contextSourceRegistrationService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { contextSourceRegistrationService.delete(any()) } returns ResourceNotFoundException("").left()

        webClient.delete()
            .uri("$csrUri/$id")
            .exchange()
            .expectStatus().isNotFound

        coVerify { contextSourceRegistrationService.delete(eq(id)) }
    }

    @Test
    fun `delete CSR should return a 204 if the deletion succeeded`() = runTest {
        coEvery { contextSourceRegistrationService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { contextSourceRegistrationService.delete(any()) } returns Unit.right()

        webClient.delete()
            .uri("$csrUri/$id")
            .exchange()
            .expectStatus().isNoContent

        coVerify { contextSourceRegistrationService.delete(eq(id)) }
    }

    @Test
    fun `create CSR should return the errors from the service`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/contextSourceRegistration_minimal_entities.json")

        coEvery { contextSourceRegistrationService.create(any()) } returns AlreadyExistsException("").left()

        webClient.post()
            .uri(csrUri)
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(409)

        coVerify(exactly = 1) { contextSourceRegistrationService.create(any()) }
    }

    @Test
    fun `create CSR should return a 204 if the creation succeeded`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/contextSourceRegistration_minimal_entities.json")

        coEvery { contextSourceRegistrationService.create(any()) } returns Unit.right()

        webClient.post()
            .uri(csrUri)
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().exists("Location")

        coVerify(exactly = 1) { contextSourceRegistrationService.create(any()) }
    }

    @Test
    fun `create CSR with context in payload should succeed`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/contextSourceRegistration.jsonld")

        coEvery { contextSourceRegistrationService.create(any()) } returns Unit.right()

        webClient.post()
            .uri(csrUri)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().exists("Location")

        coVerify(exactly = 1) { contextSourceRegistrationService.create(any()) }
    }

    @Test
    fun `update contextSourceRegistration should return a 500 if update in DB failed`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/csr_update.jsonld")
        val contextSourceRegistrationId = id
        val parsedCSR = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8).deserializeAsMap()

        coEvery {
            contextSourceRegistrationService.getById(any())
        } returns gimmeRawCSR(id = contextSourceRegistrationId).right()

        coEvery { contextSourceRegistrationService.upsert(any()) } throws RuntimeException("Update failed")

        webClient.patch()
            .uri("/ngsi-ld/v1/csourceRegistrations/$contextSourceRegistrationId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().is5xxServerError
            .expectBody().json(
                """
                    {
                        "type": "https://uri.etsi.org/ngsi-ld/errors/InternalError",
                        "title": "java.lang.RuntimeException: Update failed"
                    }
                    """
            )

        coVerify {
            contextSourceRegistrationService.upsert(
                match {
                    it.id == contextSourceRegistrationId &&
                        it.endpoint.toString() == parsedCSR["endpoint"]
                }
            )
        }
        coVerify { contextSourceRegistrationService.getById(contextSourceRegistrationId) }

        confirmVerified(contextSourceRegistrationService)
    }

    @Test
    fun `update contextSourceRegistration should return a 204 if update succeeded`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/csr_update.jsonld")
        val contextSourceRegistrationId = id
        val parsedCSR = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8).deserializeAsMap()

        coEvery { contextSourceRegistrationService.getById(any()) } returns
            gimmeRawCSR(id = contextSourceRegistrationId).right()

        coEvery { contextSourceRegistrationService.upsert(any()) } returns Unit.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/csourceRegistrations/$contextSourceRegistrationId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            contextSourceRegistrationService.upsert(
                match {
                    it.id == contextSourceRegistrationId &&
                        it.endpoint.toString() == parsedCSR["endpoint"]
                }
            )
        }
        coVerify { contextSourceRegistrationService.getById(contextSourceRegistrationId) }

        confirmVerified(contextSourceRegistrationService)
    }

    @Test
    fun `update contextSourceRegistration should return a 400 if JSON-LD context is not correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/csr_update.json")

        coEvery { contextSourceRegistrationService.getById(any()) } returns gimmeRawCSR(id = id).right()

        webClient.patch()
            .uri("/ngsi-ld/v1/csourceRegistrations/$id")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Request payload must contain @context term for a request having an application/ld+json content type"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `update contextSourceRegistration should return a 204 if JSON-LD context is provided in header`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/csr_update.json")

        coEvery { contextSourceRegistrationService.getById(any()) } returns gimmeRawCSR(id = id).right()

        coEvery { contextSourceRegistrationService.upsert(any()) } returns Unit.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/csourceRegistrations/$id")
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
    }
}
