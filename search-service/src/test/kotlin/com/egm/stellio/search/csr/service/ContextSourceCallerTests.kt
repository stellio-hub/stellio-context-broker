package com.egm.stellio.search.csr.service

import arrow.core.left
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.MiscellaneousPersistentWarning
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromGet
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.GEO_JSON_CONTENT_TYPE
import com.egm.stellio.shared.util.GEO_JSON_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.notContaining
import com.github.tomakehurst.wiremock.client.WireMock.notFound
import com.github.tomakehurst.wiremock.client.WireMock.ok
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.unauthorized
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import wiremock.com.google.common.net.HttpHeaders.ACCEPT
import wiremock.com.google.common.net.HttpHeaders.CONTENT_TYPE

@SpringBootTest
@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class ContextSourceCallerTests : WithTimescaleContainer, WithKafkaContainer {

    @SpykBean
    private lateinit var contextSourceCaller: ContextSourceCaller

    @Autowired
    private lateinit var applicationProperties: ApplicationProperties

    @MockkBean
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

    private val apiaryId = "urn:ngsi-ld:Apiary:TEST"

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
    fun `queryEntitiesFromContextSource should return the count and the entities when the request succeed`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"
        val count = 222
        val payload = "[$entityWithSysAttrs, $entityWithSysAttrs]"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE)
                        .withHeader(RESULTS_COUNT_HEADER, count.toString()).withBody(payload)
                )
        )

        val response = contextSourceCaller.queryEntitiesFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            emptyParams
        ).getOrNull()
        assertNotNull(response)
        assertEquals(count, response!!.second)
        assertJsonPayloadsAreEqual(entityWithSysAttrs, serializeObject(response.first.first()))
    }

    @Test
    fun `queryEntities ContextSource should return a RevalidationFailedWarning when receiving bad payload`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE).withBody(entityWithSysAttrs)
                )
        )

        val response = contextSourceCaller.queryEntitiesFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            emptyParams
        )

        assertTrue(response.isLeft())
        assertInstanceOf(RevalidationFailedWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `queryEntitiesFromAllSources should return the warnings sent by the CSRs and update the statuses`() = runTest {
        val csr = gimmeRawCSR()

        coEvery {
            contextSourceRegistrationService
                .getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csr, csr)

        coEvery {
            contextSourceCaller.queryEntitiesFromContextSource(any(), any(), any())
        } returns MiscellaneousWarning(
            "message with\nline\nbreaks",
            csr
        ).left() andThen
            MiscellaneousWarning("message", csr).left()

        coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

        val queryParams = MultiValueMap.fromSingleValue<String, String>(emptyMap())
        val headers = HttpHeaders()

        val (warnings, _) = contextSourceCaller.queryEntitiesFromAllContextSources(
            composeEntitiesQueryFromGet(applicationProperties.pagination, queryParams, emptyList()).getOrNull()!!,
            headers,
            queryParams
        )
        assertThat(warnings).hasSize(2)
        coVerify(exactly = 2) { contextSourceRegistrationService.updateContextSourceStatus(any(), false) }
    }

    @Test
    fun `retrieveEntityFromContextSource should return the entity when the request succeeds`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE).withBody(entityWithSysAttrs)
                )
        )

        val response = contextSourceCaller.retrieveEntityFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            apiaryId.toUri(),
            emptyParams
        )
        assertJsonPayloadsAreEqual(entityWithSysAttrs, serializeObject(response.getOrNull()!!))
    }

    @Test
    fun `retrieveEntityContextSource should return a RevalidationFailedWarning when receiving bad payload`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE).withBody(entityWithBadPayload)
                )
        )

        val response = contextSourceCaller.retrieveEntityFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            apiaryId.toUri(),
            emptyParams
        )

        assertTrue(response.isLeft())
        assertInstanceOf(RevalidationFailedWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `retrieveEntityFromAllSources should return the warnings sent by the CSRs and update the statuses`() = runTest {
        val csr = gimmeRawCSR()

        coEvery {
            contextSourceRegistrationService
                .getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csr, csr)

        coEvery {
            contextSourceCaller.retrieveEntityFromContextSource(any(), any(), any(), any())
        } returns MiscellaneousWarning(
            "message with\nline\nbreaks",
            csr
        ).left() andThen
            MiscellaneousWarning("message", csr).left()

        coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

        val (warnings, _) = contextSourceCaller.retrieveEntityFromAllContextSources(
            apiaryId.toUri(),
            HttpHeaders(),
            MultiValueMap.fromSingleValue(emptyMap())
        )
        assertThat(warnings).hasSize(2)
        coVerify(exactly = 2) { contextSourceRegistrationService.updateContextSourceStatus(any(), false) }
    }

    @Test
    fun `getDistributedInformation should return a MiscellaneousWarning if it receives no answer`() = runTest {
        val csr = gimmeRawCSR().copy(endpoint = "http://localhost:invalid".toUri())
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        val response = contextSourceCaller.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)

        assertTrue(response.isLeft())
        assertInstanceOf(MiscellaneousWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `getDistributedInformation should return MiscellaneousPersistentWarning when receiving error 401`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(unauthorized())
        )

        val response = contextSourceCaller.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)

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

        val response = contextSourceCaller.getDistributedInformation(HttpHeaders.EMPTY, csr, path, emptyParams)

        assertTrue(response.isRight())
        assertNull(response.getOrNull()!!.first)
    }

    @Test
    fun `getDistributedInformation should not ask context source for a GeoJson representation`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(ok())
        )
        val header = HttpHeaders()
        header.accept = listOf(GEO_JSON_MEDIA_TYPE)
        contextSourceCaller.getDistributedInformation(
            header,
            csr,
            path,
            emptyParams
        )
        verify(
            getRequestedFor(urlPathEqualTo(path))
                .withHeader(ACCEPT, notContaining(GEO_JSON_CONTENT_TYPE))
        )
    }

    @Test
    fun `getDistributedInformation should always ask for the normalized representation`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(notFound())
        )
        val params = LinkedMultiValueMap(mapOf(QueryParameter.OPTIONS.key to listOf("simplified")))
        contextSourceCaller.getDistributedInformation(
            HttpHeaders.EMPTY,
            csr,
            path,
            params
        )
        verify(
            getRequestedFor(urlPathEqualTo(path))
                .withQueryParam(QueryParameter.OPTIONS.key, notContaining("simplified"))
        )
    }
}
