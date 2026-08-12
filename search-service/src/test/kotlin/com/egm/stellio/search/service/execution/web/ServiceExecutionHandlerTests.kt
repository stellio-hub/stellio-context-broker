package com.egm.stellio.search.service.execution.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.service.execution.model.ServiceExecution
import com.egm.stellio.search.service.execution.model.ServiceExecutionStatus
import com.egm.stellio.search.service.execution.service.ServiceExecutionLauncher
import com.egm.stellio.search.service.execution.service.ServiceExecutionService
import com.egm.stellio.search.service.registration.model.InputInformation
import com.egm.stellio.search.service.registration.model.InputInformationType
import com.egm.stellio.search.service.registration.model.ServiceInformation
import com.egm.stellio.search.service.registration.model.ServiceInformation.ServiceMode
import com.egm.stellio.search.service.registration.model.ServiceRegistration
import com.egm.stellio.search.service.registration.service.ServiceRegistrationService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.MOCK_USER_SUB
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
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
@AutoConfigureWebTestClient(timeout = "30000")
@WebFluxTest(ServiceExecutionHandler::class)
class ServiceExecutionHandlerTests {
    @MockkBean
    private lateinit var serviceExecutionService: ServiceExecutionService

    @MockkBean
    private lateinit var serviceExecutionLauncher: ServiceExecutionLauncher

    @MockkBean
    private lateinit var serviceRegistrationService: ServiceRegistrationService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @Autowired
    private lateinit var webClient: WebTestClient

    private val resourceUri = "/ngsi-ld/v1/services"
    private val executionId = "urn:ngsi-ld:ServiceExecution:4673".toUri()
    private val serviceId = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri()
    private val execution = ServiceExecution(
        id = executionId,
        serviceId = serviceId,
        entityId = "urn:ngsi-ld:Light:001".toUri(),
        entityType = "Light",
        input = 125,
        executionStatus = ServiceExecutionStatus.SUCCESS,
        completion = 1.0,
        output = "Brightness successfully changed.",
        responseStatusCode = 200
    )
    private val synchronousRegistration = ServiceRegistration(
        id = serviceId,
        endpoint = "http://localhost:8091/invoke".toUri(),
        endpointMethod = HttpMethod.POST,
        entities = listOf(EntityInfo(types = listOf("Light"))),
        serviceInformation = ServiceInformation(
            name = "setBrightness",
            mode = ServiceMode.SYNCHRONOUS,
            input = InputInformation(type = InputInformationType.INTEGER)
        )
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
        coEvery { serviceRegistrationService.getById(serviceId) } returns synchronousRegistration.right()
        coEvery { serviceExecutionService.create(any()) } returns Unit.right()
        coEvery { serviceExecutionService.upsert(any()) } returns Unit.right()
    }

    @Test
    fun `create should return 201 with the synchronous execution result`() = runTest {
        coEvery { serviceExecutionLauncher.invoke(any(), any()) } returns execution

        webClient.post()
            .uri(resourceUri)
            .bodyValue(serviceExecutionPayload)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().valueEquals("Location", "$resourceUri/$executionId")
            .expectBody()
            .jsonPath("$.id").isEqualTo(executionId.toString())
            .jsonPath("$.executionStatus").isEqualTo("success")
            .jsonPath("$.completion").isEqualTo(1.0)
            .jsonPath("$.output").isEqualTo("Brightness successfully changed.")
            .jsonPath("$.responseStatusCode").isEqualTo(200)

        coVerify {
            serviceExecutionLauncher.invoke(
                match {
                    it.id == executionId &&
                        it.serviceId == serviceId &&
                        it.entityId == execution.entityId &&
                        it.entityType == "${NGSILD_DEFAULT_VOCAB}Light" &&
                        it.serviceName == null &&
                        it.input == 125 &&
                        it.completion == null &&
                        it.output == null &&
                        it.responseStatusCode == null &&
                        it.executionStatus == ServiceExecutionStatus.PENDING
                },
                synchronousRegistration
            )
        }
        coVerify { serviceExecutionService.upsert(execution) }
    }

    @Test
    fun `create should wait for an asynchronous service acknowledgement`() = runTest {
        val asynchronousRegistration = synchronousRegistration.copy(
            serviceInformation = synchronousRegistration.serviceInformation.copy(mode = ServiceMode.ASYNCHRONOUS)
        )
        val acknowledgedExecution = execution.copy(
            executionStatus = ServiceExecutionStatus.EXECUTING,
            completion = null,
            output = mapOf("accepted" to true),
            responseStatusCode = 202
        )
        coEvery { serviceRegistrationService.getById(serviceId) } returns asynchronousRegistration.right()
        coEvery {
            serviceExecutionLauncher.invokeAsynchronousService(any(), asynchronousRegistration)
        } returns acknowledgedExecution

        webClient.post()
            .uri(resourceUri)
            .bodyValue(serviceExecutionPayload)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().valueEquals("Location", "$resourceUri/$executionId")
            .expectBody()
            .jsonPath("$.id").isEqualTo(executionId.toString())
            .jsonPath("$.executionStatus").isEqualTo("executing")
            .jsonPath("$.completion").doesNotExist()
            .jsonPath("$.output.accepted").isEqualTo(true)
            .jsonPath("$.responseStatusCode").isEqualTo(202)

        coVerify {
            serviceExecutionService.create(
                match { it.id == executionId && it.executionStatus == ServiceExecutionStatus.PENDING }
            )
        }
        coVerify {
            serviceExecutionService.upsert(acknowledgedExecution)
        }
        coVerify {
            serviceExecutionLauncher.invokeAsynchronousService(
                match { it.id == executionId && it.executionStatus == ServiceExecutionStatus.PENDING },
                asynchronousRegistration
            )
        }
    }

    @Test
    fun `create should reject result members`() = runTest {
        listOf(
            """"completion": 0.5""",
            """"output": "Brightness successfully changed."""",
            """"responseStatusCode": 200""",
            """"executionStatus": "pending""""
        ).forEach { prohibitedMember ->
            webClient.post()
                .uri(resourceUri)
                .bodyValue(
                    serviceExecutionPayload.replace(
                        """"input": 125""",
                        """
                        "input": 125,
                          $prohibitedMember
                        """.trimIndent()
                    )
                )
                .exchange()
                .expectStatus().isBadRequest
        }

        coVerify(exactly = 0) { serviceExecutionService.create(any()) }
        coVerify(exactly = 0) { serviceExecutionLauncher.invoke(any(), any()) }
        coVerify(exactly = 0) { serviceExecutionLauncher.invokeAsynchronousService(any(), any()) }
    }

    @Test
    fun `create should validate input before launching an asynchronous service`() = runTest {
        val asynchronousRegistration = synchronousRegistration.copy(
            serviceInformation = synchronousRegistration.serviceInformation.copy(mode = ServiceMode.ASYNCHRONOUS)
        )
        coEvery { serviceRegistrationService.getById(serviceId) } returns asynchronousRegistration.right()

        webClient.post()
            .uri(resourceUri)
            .bodyValue(serviceExecutionPayload.replace("\"input\": 125", "\"input\": {}"))
            .exchange()
            .expectStatus().isBadRequest

        coVerify(exactly = 0) { serviceExecutionService.create(any()) }
        coVerify(exactly = 0) { serviceExecutionLauncher.invoke(any(), any()) }
    }

    @Test
    fun `retrieve should return status and output`() = runTest {
        coEvery { serviceExecutionService.getById(executionId) } returns execution.right()

        webClient.get()
            .uri("$resourceUri/$executionId")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isEqualTo(executionId.toString())
            .jsonPath("$.serviceId").isEqualTo(serviceId.toString())
            .jsonPath("$.executionStatus").isEqualTo("success")
            .jsonPath("$.completion").isEqualTo(1.0)
            .jsonPath("$.output").isEqualTo("Brightness successfully changed.")
            .jsonPath("$.responseStatusCode").isEqualTo(200)
            .jsonPath("$.createdAt").doesNotExist()
    }

    @Test
    fun `retrieve should propagate a not found error`() = runTest {
        coEvery {
            serviceExecutionService.getById(executionId)
        } returns ResourceNotFoundException("not found").left()

        webClient.get()
            .uri("$resourceUri/$executionId")
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    fun `update should merge executor-controlled members`() = runTest {
        coEvery { serviceExecutionService.getById(executionId) } returns execution.copy(
            executionStatus = ServiceExecutionStatus.EXECUTING,
            output = null
        ).right()
        coEvery { serviceExecutionService.upsert(any()) } returns Unit.right()

        webClient.patch()
            .uri("$resourceUri/$executionId")
            .bodyValue(
                """
                {
                  "executionStatus": "success",
                  "completion": 1.0,
                  "output": "Brightness successfully changed."
                }
                """.trimIndent()
            )
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            serviceExecutionService.upsert(
                match {
                    it.executionStatus == ServiceExecutionStatus.SUCCESS &&
                        it.completion == 1.0 &&
                        it.output == "Brightness successfully changed."
                }
            )
        }
    }

    @Test
    fun `update should reject changes to non executor-controlled members`() = runTest {
        coEvery { serviceExecutionService.getById(executionId) } returns execution.right()

        webClient.patch()
            .uri("$resourceUri/$executionId")
            .bodyValue(
                """
                {
                  "entityId": "urn:ngsi-ld:Light:002"
                }
                """.trimIndent()
            )
            .exchange()
            .expectStatus().isBadRequest

        coVerify(exactly = 0) { serviceExecutionService.upsert(any()) }
    }

    @Test
    fun `delete with remove option should delete and return 204`() = runTest {
        coEvery { serviceExecutionService.delete(executionId) } returns Unit.right()

        webClient.delete()
            .uri("$resourceUri/$executionId?options=remove")
            .exchange()
            .expectStatus().isNoContent

        coVerify { serviceExecutionService.delete(executionId) }
        coVerify(exactly = 0) { serviceExecutionLauncher.cancelExecution(any()) }
    }

    @Test
    fun `delete should default to cancel`() = runTest {
        coEvery {
            serviceExecutionLauncher.cancelExecution(executionId)
        } returns NotImplementedException("not implemented").left()

        webClient.delete()
            .uri("$resourceUri/$executionId")
            .exchange()
            .expectStatus().isEqualTo(501)

        coVerify { serviceExecutionLauncher.cancelExecution(executionId) }
        coVerify(exactly = 0) { serviceExecutionService.delete(any()) }
    }

    @Test
    fun `delete with remove and cancel options should cancel before removal`() = runTest {
        coEvery {
            serviceExecutionLauncher.cancelExecution(executionId)
        } returns NotImplementedException("not implemented").left()

        listOf("remove,cancel", "cancel,remove").forEach { options ->
            webClient.delete()
                .uri("$resourceUri/$executionId?options=$options")
                .exchange()
                .expectStatus().isEqualTo(501)
        }

        coVerify(exactly = 2) { serviceExecutionLauncher.cancelExecution(executionId) }
        coVerify(exactly = 0) { serviceExecutionService.delete(any()) }
    }

    @Test
    fun `delete should reject unsupported options`() = runTest {
        listOf(
            "$resourceUri/$executionId?options=delete",
            "$resourceUri/$executionId?options=remove,delete"
        ).forEach { uri ->
            webClient.delete()
                .uri(uri)
                .exchange()
                .expectStatus().isBadRequest
        }

        coVerify(exactly = 0) { serviceExecutionLauncher.cancelExecution(any()) }
        coVerify(exactly = 0) { serviceExecutionService.delete(any()) }
    }

    private val serviceExecutionPayload =
        """
        {
          "id": "$executionId",
          "type": "ServiceExecution",
          "serviceId": "$serviceId",
          "entityId": "urn:ngsi-ld:Light:001",
          "entityType": "Light",
          "input": 125
        }
        """.trimIndent()
}
