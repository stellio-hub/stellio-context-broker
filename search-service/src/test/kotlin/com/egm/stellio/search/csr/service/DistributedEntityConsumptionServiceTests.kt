package com.egm.stellio.search.csr.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.csr.model.MiscellaneousPersistentWarning
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromGet
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.GEO_JSON_CONTENT_TYPE
import com.egm.stellio.shared.util.GEO_JSON_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.TEMPERATURE_IRI
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
class DistributedEntityConsumptionServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @SpykBean
    private lateinit var distributedEntityConsumptionService: DistributedEntityConsumptionService

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

        val response = distributedEntityConsumptionService.queryEntitiesFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
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

        val response = distributedEntityConsumptionService.queryEntitiesFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            emptyParams
        )

        assertTrue(response.isLeft())
        assertInstanceOf(RevalidationFailedWarning::class.java, response.leftOrNull())
    }

    @Test
    fun `distributeQueryEntitiesOperation should return the warnings sent by the CSRs and update the statuses`() =
        runTest {
            val csr = gimmeRawCSR()

            coEvery {
                contextSourceRegistrationService
                    .getContextSourceRegistrations(any(), any(), any())
            } returns listOf(csr, csr)

            coEvery {
                distributedEntityConsumptionService.queryEntitiesFromContextSource(any(), any(), any(), any())
            } returns MiscellaneousWarning(
                "message with\nline\nbreaks",
                csr
            ).left() andThen
                MiscellaneousWarning("message", csr).left()

            coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

            val queryParams = MultiValueMap.fromSingleValue<String, String>(emptyMap())
            val headers = HttpHeaders()

            val (warnings, _) = distributedEntityConsumptionService.distributeQueryEntitiesOperation(
                composeEntitiesQueryFromGet(applicationProperties.pagination, queryParams, emptyList()).getOrNull()!!,
                headers,
                queryParams
            )
            assertThat(warnings).hasSize(2)
            coVerify(exactly = 2) { contextSourceRegistrationService.updateContextSourceStatus(any(), false) }
        }

    @Test
    fun `distributeQueryEntitiesOperation should ask for different filter based on the different EntityInfo`() =
        runTest {
            val idPattern = "test:.*"
            val id = "test:1".toUri()
            val firstCSR = gimmeRawCSR(
                information = listOf(
                    RegistrationInfo(
                        listOf(
                            EntityInfo(types = listOf(APIARY_IRI)),
                            EntityInfo(id = id, types = listOf(APIARY_IRI)),
                        )
                    )
                )
            )
            val secondCSR = gimmeRawCSR(
                information = listOf(
                    RegistrationInfo(
                        listOf(EntityInfo(idPattern = idPattern, types = listOf(APIARY_IRI)))
                    ),
                    RegistrationInfo(
                        listOf(EntityInfo(id = id, types = listOf(APIARY_IRI)))
                    )
                )
            )
            val thirdCSR = gimmeRawCSR(
                information = listOf(
                    RegistrationInfo(propertyNames = listOf(TEMPERATURE_IRI)),
                )
            )

            coEvery {
                contextSourceRegistrationService
                    .getContextSourceRegistrations(any(), any(), any())
            } returns listOf(firstCSR, secondCSR, thirdCSR)

            coEvery {
                distributedEntityConsumptionService.queryEntitiesFromContextSource(any(), any(), any(), any())
            } returns (emptyList<CompactedEntity>() to 0).right()

            coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

            val queryParams = MultiValueMap.fromSingleValue<String, String>(emptyMap())
            val headers = HttpHeaders()

            distributedEntityConsumptionService.distributeQueryEntitiesOperation(
                composeEntitiesQueryFromGet(applicationProperties.pagination, queryParams, emptyList()).getOrNull()!!,
                headers,
                queryParams
            )
            val typeParams = MultiValueMap.fromSingleValue(mapOf(QP.TYPE.key to APIARY_IRI))
            val idParams = MultiValueMap.fromSingleValue(mapOf(QP.TYPE.key to APIARY_IRI, QP.ID.key to id.toString()))
            val idPatternParams = MultiValueMap.fromSingleValue(
                mapOf(
                    QP.TYPE.key to APIARY_IRI,
                    QP.ID_PATTERN.key to idPattern
                )
            )
            coVerify {
                distributedEntityConsumptionService.queryEntitiesFromContextSource(any(), any(), any(), typeParams)
                distributedEntityConsumptionService.queryEntitiesFromContextSource(any(), any(), any(), idParams)
                distributedEntityConsumptionService.queryEntitiesFromContextSource(any(), any(), any(), idPatternParams)
                distributedEntityConsumptionService.queryEntitiesFromContextSource(any(), any(), any(), idParams)
                distributedEntityConsumptionService.queryEntitiesFromContextSource(any(), any(), any(), queryParams)
            }
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

        val response = distributedEntityConsumptionService.retrieveEntityFromContextSource(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
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

        val response = distributedEntityConsumptionService.retrieveEntityFromContextSource(
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
    fun `distributeRetrieveEntityOperation should return the warnings sent by the CSRs and update the statuses`() =
        runTest {
            val csr = gimmeRawCSR()

            coEvery {
                contextSourceRegistrationService
                    .getContextSourceRegistrations(any(), any(), any())
            } returns listOf(csr, csr)

            coEvery {
                distributedEntityConsumptionService.retrieveEntityFromContextSource(any(), any(), any(), any(), any())
            } returns MiscellaneousWarning(
                "message with\nline\nbreaks",
                csr
            ).left() andThen
                MiscellaneousWarning("message", csr).left()

            coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit

            val (warnings, _) = distributedEntityConsumptionService.distributeRetrieveEntityOperation(
                apiaryId.toUri(),
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 10, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
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
        val response = distributedEntityConsumptionService
            .getDistributedInformation(HttpHeaders.EMPTY, csr, CSRFilters(), path, emptyParams)

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

        val response = distributedEntityConsumptionService
            .getDistributedInformation(HttpHeaders.EMPTY, csr, CSRFilters(), path, emptyParams)

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

        val response =
            distributedEntityConsumptionService.getDistributedInformation(
                HttpHeaders.EMPTY,
                csr,
                CSRFilters(),
                path,
                emptyParams
            )

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
        distributedEntityConsumptionService.getDistributedInformation(
            header,
            csr,
            CSRFilters(),
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
        distributedEntityConsumptionService.getDistributedInformation(
            HttpHeaders.EMPTY,
            csr,
            CSRFilters(),
            path,
            params
        )
        verify(
            getRequestedFor(urlPathEqualTo(path))
                .withQueryParam(QueryParameter.OPTIONS.key, notContaining("simplified"))
        )
    }

    @Test
    fun `getDistributedInformation should filter the attributes based on the csr and the received request`() = runTest {
        val csr = gimmeRawCSR().copy(
            information = listOf(
                RegistrationInfo(null, listOf("a", "b", "c"), null)
            )
        ).expand(contexts = APIC_COMPOUND_CONTEXTS)
        val path = "/ngsi-ld/v1/entities/$apiaryId"

        stubFor(get(urlMatching(path)).willReturn(ok()))

        val header = HttpHeaders().apply {
            set(HttpHeaders.LINK, APIC_HEADER_LINK)
        }
        val params = LinkedMultiValueMap(mapOf(QueryParameter.ATTRS.key to listOf("c,d,e")))

        distributedEntityConsumptionService.getDistributedInformation(
            header,
            csr,
            CSRFilters(attrs = parseAndExpandQueryParameter("c,d,e", APIC_COMPOUND_CONTEXTS)),
            path,
            params
        )
        verify(
            getRequestedFor(urlPathEqualTo(path)).withQueryParam(
                QueryParameter.ATTRS.key,
                containing("c")
            )
        )
    }
}
