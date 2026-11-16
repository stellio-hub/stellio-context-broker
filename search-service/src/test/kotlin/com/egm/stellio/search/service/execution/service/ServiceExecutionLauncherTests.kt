package com.egm.stellio.search.service.execution.service

import arrow.core.right
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.service.execution.model.ServiceExecution
import com.egm.stellio.search.service.execution.model.ServiceExecutionStatus
import com.egm.stellio.search.service.registration.model.ServiceInformation
import com.egm.stellio.search.service.registration.model.ServiceRegistration
import com.egm.stellio.search.service.registration.service.ServiceRegistrationService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.serverError
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpMethod
import org.springframework.test.context.ActiveProfiles
import tools.jackson.databind.JsonNode

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ServiceExecutionLauncher::class])
@WireMockTest(httpPort = 8091)
@ActiveProfiles("test")
class ServiceExecutionLauncherTests {
    private val serviceId = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri()

    @Autowired
    private lateinit var serviceExecutionLauncher: ServiceExecutionLauncher

    @MockkBean
    private lateinit var serviceExecutionService: ServiceExecutionService

    @MockkBean
    private lateinit var serviceRegistrationService: ServiceRegistrationService

    @Test
    fun `invokeService should reject invalid input before storage and endpoint invocation`() = runTest {
        val registration = buildRegistration(HttpMethod.POST, mapper.readTree("""{"type":"integer"}"""))
        coEvery { serviceRegistrationService.getById(serviceId) } returns registration.right()

        serviceExecutionLauncher.invokeService(buildExecution("invalid")).shouldFailWith {
            it is BadRequestDataException
        }

        coVerify(exactly = 0) { serviceExecutionService.create(any()) }
        coVerify(exactly = 0) { serviceExecutionService.upsert(any()) }
        verify(0, postRequestedFor(urlPathEqualTo("/invoke")))
    }

    @Test
    fun `invokeService should accept matching input and absent schemas`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(HttpMethod.POST, mapper.readTree("""{"type":"integer"}"""))
        coEvery { serviceRegistrationService.getById(serviceId) } returns registration.right() andThen
            registration.copy(serviceInformation = registration.serviceInformation.copy(input = null)).right()
        coEvery { serviceExecutionService.create(any()) } returns Unit.right()
        coEvery { serviceExecutionService.upsert(any()) } returns Unit.right()
        stubFor(post(urlPathEqualTo("/invoke")).willReturn(okJson("true")))

        serviceExecutionLauncher.invokeService(execution).shouldSucceed()
        serviceExecutionLauncher.invokeService(buildExecution("no schema")).shouldSucceed()

        coVerify(exactly = 2) { serviceExecutionService.create(any()) }
        verify(2, postRequestedFor(urlPathEqualTo("/invoke")))
    }

    @Test
    fun `invokeSynchronousService should post and return a successful execution`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(
            HttpMethod.POST,
            mapper.readTree("""{"type":"integer","minimum":0,"maximum":255}""")
        )
        stubFor(
            post(urlPathEqualTo("/invoke"))
                .willReturn(okJson("""{"accepted":true}"""))
        )

        val successfulExecution = serviceExecutionLauncher.invokeSynchronousService(execution, registration)

        assertEquals(ServiceExecutionStatus.SUCCESS, successfulExecution.executionStatus)
        assertEquals(null, successfulExecution.progress)
        assertEquals(mapOf("accepted" to true), successfulExecution.output)
        assertEquals(200, successfulExecution.responseStatusCode)
        verify(
            postRequestedFor(urlPathEqualTo("/invoke"))
                .withHeader("Service-Execution", equalTo(execution.id.toString()))
                .withRequestBody(equalTo("125"))
        )
    }

    @Test
    fun `invokeSynchronousService should use the registered GET method`() = runTest {
        val execution = buildExecution("turn-on")
        val registration = buildRegistration(
            HttpMethod.GET,
            mapper.readTree("""{"type":"string"}""")
        )
        stubFor(
            get(urlPathEqualTo("/invoke"))
                .willReturn(okJson("\"done\""))
        )

        val successfulExecution = serviceExecutionLauncher.invokeSynchronousService(execution, registration)

        assertEquals("done", successfulExecution.output)
        assertEquals(200, successfulExecution.responseStatusCode)
        verify(
            getRequestedFor(urlPathEqualTo("/invoke"))
                .withHeader("Service-Execution", equalTo(execution.id.toString()))
                .withRequestBody(equalTo("\"turn-on\""))
        )
    }

    @Test
    fun `invokeSynchronousService should return failure when the endpoint responds with an error`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(
            HttpMethod.POST,
            mapper.readTree("""{"type":"integer"}""")
        )
        stubFor(
            post(urlPathEqualTo("/invoke"))
                .willReturn(serverError().withBody("""{"error":"execution rejected"}"""))
        )

        val failedExecution = serviceExecutionLauncher.invokeSynchronousService(execution, registration)

        assertEquals(ServiceExecutionStatus.FAILURE, failedExecution.executionStatus)
        assertEquals(mapOf("error" to "execution rejected"), failedExecution.output)
        assertEquals(500, failedExecution.responseStatusCode)
    }

    @Test
    fun `invokeAsynchronousService should wait for and return the endpoint acknowledgement`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(
            HttpMethod.POST,
            mapper.readTree("""{"type":"integer"}""")
        )
        stubFor(
            post(urlPathEqualTo("/invoke"))
                .willReturn(okJson("""{"accepted":true}"""))
        )

        val successfulExecution = serviceExecutionLauncher.invokeAsynchronousService(execution, registration)

        assertEquals(ServiceExecutionStatus.EXECUTING, successfulExecution.executionStatus)
        assertEquals(mapOf("accepted" to true), successfulExecution.output)
        assertEquals(200, successfulExecution.responseStatusCode)
        verify(
            postRequestedFor(urlPathEqualTo("/invoke"))
                .withHeader("Service-Execution", equalTo(execution.id.toString()))
                .withRequestBody(equalTo("125"))
        )
    }

    @Test
    fun `invokeAsynchronousService should capture a failed acknowledgement response`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(
            HttpMethod.POST,
            mapper.readTree("""{"type":"integer"}""")
        )
        stubFor(
            post(urlPathEqualTo("/invoke"))
                .willReturn(serverError().withBody("execution rejected"))
        )

        val failedExecution = serviceExecutionLauncher.invokeAsynchronousService(execution, registration)

        assertEquals(ServiceExecutionStatus.FAILURE, failedExecution.executionStatus)
        assertEquals("execution rejected", failedExecution.output)
        assertEquals(500, failedExecution.responseStatusCode)
    }

    private fun buildExecution(input: Any) =
        ServiceExecution(
            id = "urn:ngsi-ld:ServiceExecution:4673".toUri(),
            serviceId = serviceId,
            entityId = "urn:ngsi-ld:Light:001".toUri(),
            entityType = "Light",
            input = input
        )

    private fun buildRegistration(
        endpointMethod: HttpMethod,
        inputSchema: JsonNode
    ) =
        ServiceRegistration(
            id = serviceId,
            endpoint = "http://localhost:8091/invoke".toUri(),
            endpointMethod = endpointMethod,
            entities = listOf(EntityInfo(types = listOf("Light"))),
            serviceInformation = ServiceInformation(
                name = "setBrightness",
                input = inputSchema
            )
        )
}
