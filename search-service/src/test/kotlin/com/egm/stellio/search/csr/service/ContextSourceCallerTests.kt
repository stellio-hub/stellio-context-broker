package com.egm.stellio.search.csr.service

import com.egm.stellio.search.csr.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import wiremock.com.google.common.net.HttpHeaders.CONTENT_TYPE

@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class ContextSourceCallerTests {

    private val apiaryId = "urn:ngsi-ld:Apiary:TEST"

    private fun gimmeRawCSR() = ContextSourceRegistration(
        id = "urn:ngsi-ld:ContextSourceRegistration:test".toUri(),
        endpoint = "http://localhost:8089".toUri(),
        information = emptyList(),
        operations = listOf(Operation.FEDERATION_OPS),
        createdAt = ngsiLdDateTime(),
    )
    private val emptyParams = LinkedMultiValueMap<String, String>()
    private val entityWithSysAttrs =
        """
        {
           "id":"$apiaryId",
           "type":"Apiary",
           "createdAt": "2024-02-13T18:15:00Z",
           "modifiedAt": "2024-02-13T18:16:00Z",
           "name": {
              "type":"Property",
              "value":"ApiarySophia",
              "createdAt": "2024-02-13T18:15:00Z",
              "modifiedAt": "2024-02-13T18:16:00Z"
           },
           "@context":[ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

    private val entityWithBadPayload =
        """
        {
           "id":"$apiaryId",
           "type":"Apiary",
           "name": {
              "type":"Property",
              "value":"ApiarySophia",
           ,
           "@context":[ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

    @Test
    fun `getDistributedInformation should return the entity when the request succeed`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE).withBody(entityWithSysAttrs)
                )
        )

        val response = ContextSourceCaller.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)
        assertJsonPayloadsAreEqual(entityWithSysAttrs, serializeObject(response.getOrNull()!!))
    }

    @Test
    fun `getDistributedInformation should return a MiscellaneousWarning if it receive no answer`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"

        val response = ContextSourceCaller.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)

        assertTrue(response.isLeft())
        assertInstanceOf(MiscellaneousWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `getDistributedInformation should return a RevalidationFailedWarning when receiving a bad payload`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE).withBody(entityWithBadPayload)
                )
        )

        val response = ContextSourceCaller.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)

        assertTrue(response.isLeft())
        assertInstanceOf(RevalidationFailedWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `getDistributedInformation should return MiscellaneousPersistentWarning when receiving error 500`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(unauthorized())
        )

        val response = ContextSourceCaller.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)

        assertTrue(response.isLeft())
        assertInstanceOf(MiscellaneousPersistentWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `getDistributedInformation should return null when receiving an error 404`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(notFound())
        )

        val response = ContextSourceCaller.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)

        assertTrue(response.isRight())
        assertNull(response.leftOrNull())
    }
}
