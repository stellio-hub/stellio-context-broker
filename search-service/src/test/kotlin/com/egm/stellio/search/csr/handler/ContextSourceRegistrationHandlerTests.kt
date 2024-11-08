package com.egm.stellio.search.csr.handler

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.service.ContextSourceRegistrationService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.AQUAC_HEADER_LINK
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeAll
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

        coEvery { contextSourceRegistrationService.create(any(), any()) } returns AlreadyExistsException("").left()

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

        coVerify(exactly = 1) { contextSourceRegistrationService.create(any(), any()) }
    }

    @Test
    fun `create CSR should return a 204 if the creation succeeded`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/contextSourceRegistration_minimal_entities.json")

        coEvery { contextSourceRegistrationService.create(any(), any()) } returns Unit.right()

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

        coVerify(exactly = 1) { contextSourceRegistrationService.create(any(), any()) }
    }

    @Test
    fun `create CSR with context in payload should succeed`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/csr/contextSourceRegistration.jsonld")

        coEvery { contextSourceRegistrationService.create(any(), any()) } returns Unit.right()

        webClient.post()
            .uri(csrUri)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().exists("Location")

        coVerify(exactly = 1) { contextSourceRegistrationService.create(any(), any()) }
    }
}
