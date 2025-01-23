package com.egm.stellio.search.csr.service

import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.ErrorType
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.client.WireMock.badRequest
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.SpykBean
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class DistributedEntityProvisionServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @SpykBean
    private lateinit var distributedEntityProvisionService: DistributedEntityProvisionService

//    @Autowired
//    private lateinit var applicationProperties: ApplicationProperties
//
//    @MockkBean
//    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

    private val apiaryId = "urn:ngsi-ld:Apiary:TEST"

    private val entity =
        """
        {
           "id":"$apiaryId",
           "type":"Apiary",
           "name": {
              "type":"Property",
              "value":"ApiarySophia"
           },
           "@context":[ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

    private val validErrorResponse =
        """
        {
           "type":"${ErrorType.BAD_REQUEST_DATA.type}",
           "status": ${HttpStatus.BAD_REQUEST.value()},
           "title": "A valid error",
           "detail":"The detail of the valid Error"
        }
        """.trimIndent()

    private val responseWithBadPayload = "error message not respecting specification"

    @Test
    fun `postDistributedInformation should process badly formed errors`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"

        stubFor(
            post(urlMatching(path))
                .willReturn(badRequest().withBody(responseWithBadPayload))
        )

        val entity = compactEntity(expandJsonLdEntity(entity), listOf(APIC_COMPOUND_CONTEXT))
        val response = distributedEntityProvisionService.postDistributedInformation(HttpHeaders(), entity, csr, path)

        assertTrue(response.isLeft())
        assertEquals(response.leftOrNull()?.type, ErrorType.BAD_GATEWAY.type)
        assertEquals(response.leftOrNull()?.status, HttpStatus.BAD_GATEWAY)
        assertEquals(response.leftOrNull()?.detail, responseWithBadPayload)
    }

    @Test
    fun `postDistributedInformation should return the received error`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"

        stubFor(
            post(urlMatching(path))
                .willReturn(badRequest().withBody(validErrorResponse))
        )

        val entity = compactEntity(expandJsonLdEntity(entity), listOf(APIC_COMPOUND_CONTEXT))
        val response = distributedEntityProvisionService.postDistributedInformation(HttpHeaders(), entity, csr, path)

        assertTrue(response.isLeft())
        assertInstanceOf(ContextSourceException::class.java, response.leftOrNull())
        assertEquals(response.leftOrNull()?.type, ErrorType.BAD_REQUEST_DATA.type)
        assertEquals(response.leftOrNull()?.status, HttpStatus.BAD_REQUEST)
        assertEquals(response.leftOrNull()?.detail, "The detail of the valid Error")
        assertEquals(response.leftOrNull()?.message, "A valid error")
    }

    @Test
    fun `postDistributedInformation should return a GateWayTimeOut if it receives no answer`() = runTest {
        val csr = gimmeRawCSR().copy(endpoint = "http://localhost:invalid".toUri())
        val path = "/ngsi-ld/v1/entities"
        val entity = compactEntity(expandJsonLdEntity(entity), listOf(APIC_COMPOUND_CONTEXT))
        val response = distributedEntityProvisionService.postDistributedInformation(HttpHeaders(), entity, csr, path)

        assertTrue(response.isLeft())
        assertInstanceOf(GatewayTimeoutException::class.java, response.leftOrNull())
    }
}
