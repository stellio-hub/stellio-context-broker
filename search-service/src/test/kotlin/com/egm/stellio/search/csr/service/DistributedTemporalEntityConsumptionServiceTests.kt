package com.egm.stellio.search.csr.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.MiscellaneousPersistentWarning
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.util.composeTemporalEntitiesQueryFromGet
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.parseAndExpandQueryParameter
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.client.WireMock.containing
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
import com.ninjasquad.springmockk.MockkSpyBean
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
import wiremock.com.google.common.net.HttpHeaders.CONTENT_TYPE

@SpringBootTest
@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class DistributedTemporalEntityConsumptionServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @MockkSpyBean
    private lateinit var distributedTemporalEntityConsumptionService: DistributedTemporalEntityConsumptionService

    @Autowired
    private lateinit var applicationProperties: ApplicationProperties

    @MockkBean
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

    private val apiaryId = "urn:ngsi-ld:Apiary:TEST"

    private val emptyParams = LinkedMultiValueMap<String, String>()

    private val temporalEntityPayload =
        """
        {
           "id":"$apiaryId",
           "type":"Apiary",
           "temperature": [
               {"type":"Property","value":22.5,"instanceId":"urn:ngsi-ld:Instance:1","observedAt":"2024-01-01T00:00:00Z"},
               {"type":"Property","value":23.0,"instanceId":"urn:ngsi-ld:Instance:2","observedAt":"2024-01-02T00:00:00Z"}
           ],
           "@context":[ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

    private val temporalEntityListPayload = "[$temporalEntityPayload]"

    private val badPayload =
        """
        {
           "id":"$apiaryId",
           "type":"Apiary",
           "temperature": {
              "type":"Property",
              "value":"bad",
           ,
           "@context":[ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

    private fun buildTemporalEntitiesQuery(
        queryParams: MultiValueMap<String, String> = emptyParams
    ): TemporalEntitiesQueryFromGet =
        composeTemporalEntitiesQueryFromGet(
            applicationProperties.pagination,
            queryParams,
            APIC_COMPOUND_CONTEXTS
        ).getOrNull()!!

    @Test
    fun `retrieveTemporalEntityFromContextSource should return the entity when the request succeeds`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/temporal/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(ok().withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE).withBody(temporalEntityPayload))
        )

        val response = distributedTemporalEntityConsumptionService.retrieveTemporalEntityFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            apiaryId.toUri(),
            emptyParams
        )

        assertJsonPayloadsAreEqual(temporalEntityPayload, serializeObject(response.getOrNull()!!))
    }

    @Test
    fun `retrieveTemporalEntityFromContextSource should return null on 404`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/temporal/entities/$apiaryId"
        stubFor(get(urlMatching(path)).willReturn(notFound()))

        val response = distributedTemporalEntityConsumptionService.retrieveTemporalEntityFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            apiaryId.toUri(),
            emptyParams
        )

        assertTrue(response.isRight())
        assertNull(response.getOrNull())
    }

    @Test
    fun `retrieveTemporalEntityFromContextSource should return RevalidationFailedWarning on bad payload`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/temporal/entities/$apiaryId"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE)
                        .withBody(badPayload)
                )
        )

        val response = distributedTemporalEntityConsumptionService.retrieveTemporalEntityFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            apiaryId.toUri(),
            emptyParams
        )

        assertTrue(response.isLeft())
        assertInstanceOf(RevalidationFailedWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `distributeRetrieveTemporalEntityOperation should return entities from CSRs and update status to true`() =
        runTest {
            val csr = gimmeRawCSR()
            val remoteEntity = mapOf("id" to apiaryId, "type" to "Apiary")

            coEvery {
                contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
            } returns listOf(csr)

            coEvery {
                distributedTemporalEntityConsumptionService.retrieveTemporalEntityFromContextSource(
                    any(), any(), any(), any(), any()
                )
            } returns remoteEntity.right()

            coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

            val (warnings, entities) =
                distributedTemporalEntityConsumptionService.distributeRetrieveTemporalEntityOperation(
                    apiaryId.toUri(),
                    buildTemporalEntitiesQuery(),
                    HttpHeaders(),
                    emptyParams
                )

            assertThat(warnings).isEmpty()
            assertThat(entities).hasSize(1)
            assertEquals(remoteEntity, entities.first().first)
            coVerify(exactly = 1) { contextSourceRegistrationService.updateContextSourceStatus(any(), true) }
        }

    @Test
    fun `distributeRetrieveTemporalEntityOperation should return warnings and update CSR statuses`() = runTest {
        val csr = gimmeRawCSR()

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csr, csr)

        coEvery {
            distributedTemporalEntityConsumptionService.retrieveTemporalEntityFromContextSource(
                any(), any(), any(), any(), any()
            )
        } returns MiscellaneousWarning("error1", csr).left() andThen
            MiscellaneousWarning("error2", csr).left()

        coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

        val (warnings, _) = distributedTemporalEntityConsumptionService.distributeRetrieveTemporalEntityOperation(
            apiaryId.toUri(),
            buildTemporalEntitiesQuery(),
            HttpHeaders(),
            emptyParams
        )

        assertThat(warnings).hasSize(2)
        coVerify(exactly = 2) { contextSourceRegistrationService.updateContextSourceStatus(any(), false) }
    }

    @Test
    fun `queryTemporalEntitiesFromContextSource should return entities`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/temporal/entities"
        stubFor(
            get(urlMatching(path))
                .willReturn(
                    ok()
                        .withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE)
                        .withBody(temporalEntityListPayload)
                )
        )

        val response = distributedTemporalEntityConsumptionService.queryTemporalEntitiesFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            emptyParams
        ).getOrNull()

        assertNotNull(response)
        assertJsonPayloadsAreEqual(temporalEntityPayload, serializeObject(response!!.first.first()))
    }

    @Test
    fun `queryTemporalEntitiesFromContextSource should return RevalidationFailedWarning on bad payload`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/temporal/entities"
        stubFor(
            get(urlMatching(path))
                .willReturn(ok().withHeader(CONTENT_TYPE, APPLICATION_JSON_VALUE).withBody(temporalEntityPayload))
        )

        val response = distributedTemporalEntityConsumptionService.queryTemporalEntitiesFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            emptyParams
        )

        assertTrue(response.isLeft())
        assertInstanceOf(RevalidationFailedWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `distributeQueryTemporalEntitiesOperation should return entities and counts on success`() =
        runTest {
            val csr = gimmeRawCSR()
            val remoteEntity = mapOf("id" to apiaryId, "type" to "Apiary")
            val count = 5

            coEvery {
                contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
            } returns listOf(csr)

            coEvery {
                distributedTemporalEntityConsumptionService.queryTemporalEntitiesFromContextSource(
                    any(), any(), any(), any()
                )
            } returns (listOf(remoteEntity) to count).right()

            coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

            val (warnings, entitiesWithCSR, counts) =
                distributedTemporalEntityConsumptionService.distributeQueryTemporalEntitiesOperation(
                    buildTemporalEntitiesQuery(),
                    HttpHeaders(),
                    emptyParams
                )

            assertThat(warnings).isEmpty()
            assertThat(entitiesWithCSR).hasSize(1)
            assertThat(counts).containsExactly(count)
            coVerify(exactly = 1) { contextSourceRegistrationService.updateContextSourceStatus(any(), true) }
        }

    @Test
    fun `distributeQueryTemporalEntitiesOperation should return warnings and update CSR statuses`() = runTest {
        val csr = gimmeRawCSR()

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csr, csr)

        coEvery {
            distributedTemporalEntityConsumptionService.queryTemporalEntitiesFromContextSource(
                any(), any(), any(), any()
            )
        } returns MiscellaneousWarning("error1", csr).left() andThen
            MiscellaneousWarning("error2", csr).left()

        coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

        val (warnings, _, _) = distributedTemporalEntityConsumptionService.distributeQueryTemporalEntitiesOperation(
            buildTemporalEntitiesQuery(),
            HttpHeaders(),
            emptyParams
        )

        assertThat(warnings).hasSize(2)
        coVerify(exactly = 2) { contextSourceRegistrationService.updateContextSourceStatus(any(), false) }
    }

    @Test
    fun `getTemporalDistributedInformation should preserve options parameter`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/temporal/entities/$apiaryId"
        stubFor(get(urlMatching(path)).willReturn(ok()))

        val params = LinkedMultiValueMap(mapOf(QueryParameter.OPTIONS.key to listOf("temporalValues")))

        distributedTemporalEntityConsumptionService.getTemporalDistributedInformation(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            path,
            params
        )

        verify(
            getRequestedFor(urlPathEqualTo(path))
                .withQueryParam(QueryParameter.OPTIONS.key, containing("temporalValues"))
        )
    }

    @Test
    fun `getTemporalDistributedInformation should strip geometryProperty parameter`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/temporal/entities"
        stubFor(get(urlMatching(path)).willReturn(ok()))

        val params = LinkedMultiValueMap(
            mapOf(
                QueryParameter.GEOMETRY_PROPERTY.key to listOf("location"),
                QP.TIMEREL.key to listOf("before"),
                QP.TIMEAT.key to listOf("2024-01-01T00:00:00Z")
            )
        )

        distributedTemporalEntityConsumptionService.getTemporalDistributedInformation(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            path,
            params
        )

        verify(
            getRequestedFor(urlPathEqualTo(path))
                .withQueryParam(QueryParameter.GEOMETRY_PROPERTY.key, notContaining("geometryProperty"))
        )
    }

    @Test
    fun `getTemporalDistributedInformation should return MiscellaneousPersistentWarning on error response`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/temporal/entities/$apiaryId"
        stubFor(get(urlMatching(path)).willReturn(unauthorized()))

        val response = distributedTemporalEntityConsumptionService.getTemporalDistributedInformation(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            path,
            emptyParams
        )

        assertTrue(response.isLeft())
        assertInstanceOf(MiscellaneousPersistentWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `getTemporalDistributedInformation should return MiscellaneousWarning when CSR is unreachable`() = runTest {
        val csr = gimmeRawCSR().copy(endpoint = "http://localhost:invalid".toUri())
        val path = "/ngsi-ld/v1/temporal/entities/$apiaryId"

        val response = distributedTemporalEntityConsumptionService.getTemporalDistributedInformation(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            path,
            emptyParams
        )

        assertTrue(response.isLeft())
        assertInstanceOf(MiscellaneousWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `getTemporalDistributedInformation should filter attrs based on CSR information`() = runTest {
        val csr = gimmeRawCSR().copy(
            information = listOf(
                RegistrationInfo(null, listOf("temperature"), null)
            )
        ).expand(contexts = APIC_COMPOUND_CONTEXTS)
        val path = "/ngsi-ld/v1/temporal/entities"

        stubFor(get(urlMatching(path)).willReturn(ok()))

        val headers = HttpHeaders().apply { set(HttpHeaders.LINK, APIC_HEADER_LINK) }
        val params = LinkedMultiValueMap(mapOf(QueryParameter.ATTRS.key to listOf("temperature,humidity")))

        distributedTemporalEntityConsumptionService.getTemporalDistributedInformation(
            headers,
            csr,
            CSRFilters(attrs = parseAndExpandQueryParameter("temperature,humidity", APIC_COMPOUND_CONTEXTS)),
            path,
            params
        )

        verify(
            getRequestedFor(urlPathEqualTo(path))
                .withQueryParam(QueryParameter.ATTRS.key, containing("temperature"))
        )
    }
}
