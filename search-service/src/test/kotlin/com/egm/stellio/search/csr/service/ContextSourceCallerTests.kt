package com.egm.stellio.search.csr.service

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import kotlinx.coroutines.test.runTest
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

    fun gimmeRawCSR() = ContextSourceRegistration(
        id = "urn:ngsi-ld:ContextSourceRegistration:test".toUri(),
        endpoint = "http://localhost:8089".toUri(),
        information = emptyList(),
        operations = listOf(Operation.FEDERATION_OPS),
        createdAt = ngsiLdDateTime(),

    )
    val emptyParams = LinkedMultiValueMap<String, String>()
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

        val response = ContextSourceUtils.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)
        assertJsonPayloadsAreEqual(entityWithSysAttrs, serializeObject(response.getOrNull()!!))
    }

    @Test
    fun `getDistributedInformation should fail with a `() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE).withBody(entityWithSysAttrs)
                )
        )

        val response = ContextSourceUtils.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)
        assertJsonPayloadsAreEqual(entityWithSysAttrs, serializeObject(response.getOrNull()!!))
    }
}
