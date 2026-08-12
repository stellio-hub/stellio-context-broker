package com.egm.stellio.search.service.execution.service

import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.service.execution.model.ServiceExecution
import com.egm.stellio.search.service.execution.model.ServiceExecutionStatus
import com.egm.stellio.search.service.registration.model.InputInformation
import com.egm.stellio.search.service.registration.model.InputInformationType
import com.egm.stellio.search.service.registration.model.ServiceInformation
import com.egm.stellio.search.service.registration.model.ServiceRegistration
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
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.http.HttpMethod

@WireMockTest(httpPort = 8091)
class ServiceExecutionLauncherTests {
    private val serviceExecutionLauncher = ServiceExecutionLauncher()

    private val serviceId = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri()

    @Test
    fun `invoke should post and return a successful execution`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(
            HttpMethod.POST,
            InputInformation(
                type = InputInformationType.INTEGER,
                minimum = 0.toBigDecimal(),
                maximum = 255.toBigDecimal()
            )
        )
        stubFor(
            post(urlPathEqualTo("/invoke"))
                .willReturn(okJson("""{"accepted":true}"""))
        )

        val successfulExecution = serviceExecutionLauncher.invoke(execution, registration)

        assertEquals(ServiceExecutionStatus.SUCCESS, successfulExecution.executionStatus)
        assertEquals(null, successfulExecution.completion)
        assertEquals(mapOf("accepted" to true), successfulExecution.output)
        assertEquals(200, successfulExecution.responseStatusCode)
        verify(
            postRequestedFor(urlPathEqualTo("/invoke"))
                .withRequestBody(equalTo("125"))
        )
    }

    @Test
    fun `invoke should use the registered GET method`() = runTest {
        val execution = buildExecution("turn-on")
        val registration = buildRegistration(
            HttpMethod.GET,
            InputInformation(type = InputInformationType.STRING)
        )
        stubFor(
            get(urlPathEqualTo("/invoke"))
                .willReturn(okJson("\"done\""))
        )

        val successfulExecution = serviceExecutionLauncher.invoke(execution, registration)

        assertEquals("done", successfulExecution.output)
        assertEquals(200, successfulExecution.responseStatusCode)
        verify(
            getRequestedFor(urlPathEqualTo("/invoke"))
                .withRequestBody(equalTo("\"turn-on\""))
        )
    }

    @Test
    fun `invoke should return failure when the endpoint responds with an error`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(
            HttpMethod.POST,
            InputInformation(type = InputInformationType.INTEGER)
        )
        stubFor(
            post(urlPathEqualTo("/invoke"))
                .willReturn(serverError().withBody("""{"error":"execution rejected"}"""))
        )

        val failedExecution = serviceExecutionLauncher.invoke(execution, registration)

        assertEquals(ServiceExecutionStatus.FAILURE, failedExecution.executionStatus)
        assertEquals(mapOf("error" to "execution rejected"), failedExecution.output)
        assertEquals(500, failedExecution.responseStatusCode)
    }

    @Test
    fun `invokeAsynchronousService should wait for and return the endpoint acknowledgement`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(
            HttpMethod.POST,
            InputInformation(type = InputInformationType.INTEGER)
        )
        stubFor(
            post(urlPathEqualTo("/invoke"))
                .willReturn(okJson("""{"accepted":true}"""))
        )

        val successfulExecution = serviceExecutionLauncher.invokeAsynchronousService(execution, registration)

        assertEquals(ServiceExecutionStatus.EXECUTING, successfulExecution.executionStatus)
        assertEquals(mapOf("accepted" to true), successfulExecution.output)
        assertEquals(200, successfulExecution.responseStatusCode)
    }

    @Test
    fun `invokeAsynchronousService should capture a failed acknowledgement response`() = runTest {
        val execution = buildExecution(125)
        val registration = buildRegistration(
            HttpMethod.POST,
            InputInformation(type = InputInformationType.INTEGER)
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
        inputInformation: InputInformation
    ) =
        ServiceRegistration(
            id = serviceId,
            endpoint = "http://localhost:8091/invoke".toUri(),
            endpointMethod = endpointMethod,
            entities = listOf(EntityInfo(types = listOf("Light"))),
            serviceInformation = ServiceInformation(
                name = "setBrightness",
                input = inputInformation
            )
        )
}
