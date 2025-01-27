package com.egm.stellio.search.csr.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.RegistrationInfoFilter
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.ConflictException
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.ErrorType
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.TEMPERATURE_PROPERTY
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.client.WireMock.badRequest
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.coEvery
import io.mockk.coVerifyOrder
import io.mockk.spyk
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class DistributedEntityProvisionServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @SpykBean
    private lateinit var distributedEntityProvisionService: DistributedEntityProvisionService

    @MockkBean
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

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
                      "name": {
                         "type":"Property",
                         "value":"ApiarySophia"
                      },
           "temperature":{
              "type":"Property",
              "value":"19",
              "unitCode":"Cel"
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

    private val invalidErrorResponse = "error message not respecting specification"

    private val contextSourceException = ContextSourceException(
        type = ErrorType.BAD_REQUEST_DATA.type,
        status = HttpStatus.MULTI_STATUS,
        title = "A valid error",
        detail = "The detail of the valid Error"
    )
    private val contexts = listOf(APIC_COMPOUND_CONTEXT)

    @Test
    fun `distributeCreateEntity should return the errors received and the remainingEntity`() = runTest {
        val firstExclusiveCsr = gimmeRawCSR(id = "id:exclusive:1".toUri(), mode = Mode.EXCLUSIVE)
        val firstRedirectCsr = gimmeRawCSR(id = "id:redirect:1".toUri(), mode = Mode.REDIRECT)
        val firstInclusiveCsr = gimmeRawCSR(id = "id:inclusive:1".toUri(), mode = Mode.INCLUSIVE)
        val secondRedirectCsr = gimmeRawCSR(id = "id:redirect:2".toUri(), mode = Mode.REDIRECT)
        val secondInclusiveCsr = gimmeRawCSR(id = "id:inclusive:2;".toUri(), mode = Mode.INCLUSIVE)

        val entryEntity = expandJsonLdEntity(entity)
        val entityWithIgnoredTemperature = entryEntity.omitAttributes(setOf(TEMPERATURE_PROPERTY))
        val entityWithIgnoredTemperatureAndName = entryEntity.omitAttributes(setOf(NGSILD_NAME_PROPERTY))

        val firstExclusiveStatus = (ConflictException("conflict") to firstExclusiveCsr).left()
        val firstRedirectStatus = (BadGatewayException("badGateway") to firstRedirectCsr).left()
        val secondInclusiveStatus = (ConflictException("conflict") to secondInclusiveCsr).left()

        coEvery {
            distributedEntityProvisionService.distributeCreateEntityForContextSources(any(), any(), any(), any())
        } returns
            (listOf(firstExclusiveStatus) to entityWithIgnoredTemperature) andThen
            (listOf(firstRedirectStatus, Unit.right()) to entityWithIgnoredTemperatureAndName) andThen
            (listOf(Unit.right(), secondInclusiveStatus) to null)

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(firstInclusiveCsr, firstExclusiveCsr, firstRedirectCsr, secondInclusiveCsr, secondRedirectCsr)

        val (distributionsStatuses, remainingEntity) = distributedEntityProvisionService.distributeCreateEntity(
            entryEntity,
            contexts
        )
        assertThat(distributionsStatuses)
            .contains(Unit.right(), firstRedirectStatus, firstExclusiveStatus, secondInclusiveStatus)
            .size().isEqualTo(5)

        assertEquals(entityWithIgnoredTemperatureAndName, remainingEntity)

        coVerifyOrder {
            distributedEntityProvisionService.distributeCreateEntityForContextSources(
                listOf(firstExclusiveCsr),
                any(),
                entryEntity,
                any(),
            )
            distributedEntityProvisionService.distributeCreateEntityForContextSources(
                listOf(firstRedirectCsr, secondRedirectCsr),
                any(),
                entityWithIgnoredTemperature,
                any(),
            )
            distributedEntityProvisionService.distributeCreateEntityForContextSources(
                listOf(firstInclusiveCsr, secondInclusiveCsr),
                any(),
                entityWithIgnoredTemperatureAndName,
                any(),
            )
        }
    }

    @Test
    fun `distributeCreateEntityForContextSources should return received errors and successes`() = runTest {
        val csr = spyk(gimmeRawCSR())

        coEvery {
            distributedEntityProvisionService.postDistributedInformation(any(), any(), any())
        } returns contextSourceException.left() andThen Unit.right()

        coEvery {
            csr.getAssociatedAttributes(any(), any())
        } returns setOf(NGSILD_NAME_PROPERTY) andThen setOf(TEMPERATURE_PROPERTY)

        val (distributionsStatuses, _) = distributedEntityProvisionService.distributeCreateEntityForContextSources(
            listOf(csr, csr),
            RegistrationInfoFilter(),
            expandJsonLdEntity(entity),
            contexts
        )

        val firstStatus = distributionsStatuses.first()
        val secondStatus = distributionsStatuses.getOrNull(1)!!
        assertTrue(firstStatus.isLeft())
        assertTrue(secondStatus.isRight())

        assertEquals(firstStatus.leftOrNull()?.first, contextSourceException)
    }

    @Test
    fun `distributeCreateEntityForContextSources should return null if all the entity have been processed`() = runTest {
        val csr = spyk(gimmeRawCSR())
        coEvery {
            csr.getAssociatedAttributes(any(), any())
        } returns setOf(NGSILD_NAME_PROPERTY) andThen setOf(TEMPERATURE_PROPERTY)

        coEvery {
            distributedEntityProvisionService.postDistributedInformation(any(), any(), any())
        } returns contextSourceException.left() andThen Unit.right()

        val (_, entity) = distributedEntityProvisionService.distributeCreateEntityForContextSources(
            listOf(csr, csr),
            RegistrationInfoFilter(),
            expandJsonLdEntity(entity),
            contexts
        )

        assertNull(entity)
    }

    @Test
    fun `distributeCreateEntityForContextSources should return only non processed attributes`() = runTest {
        val csr = spyk(gimmeRawCSR())
        coEvery {
            csr.getAssociatedAttributes(any(), any())
        } returns setOf(NGSILD_NAME_PROPERTY)

        coEvery {
            distributedEntityProvisionService.postDistributedInformation(any(), any(), any())
        } returns Unit.right()

        val (_, successEntity) = distributedEntityProvisionService.distributeCreateEntityForContextSources(
            listOf(csr),
            RegistrationInfoFilter(),
            expandJsonLdEntity(entity),
            contexts
        )

        assertNull(successEntity?.getAttributes()?.get(NGSILD_NAME_PROPERTY))
        assertNotNull(successEntity?.getAttributes()?.get(TEMPERATURE_PROPERTY))
    }

    @Test
    fun `distributeCreateEntityForContextSources should remove the attrs even when the csr is in error`() = runTest {
        val csr = spyk(gimmeRawCSR())

        coEvery {
            csr.getAssociatedAttributes(any(), any())
        } returns setOf(NGSILD_NAME_PROPERTY)
        coEvery {
            distributedEntityProvisionService.postDistributedInformation(any(), any(), any())
        } returns contextSourceException.left()

        val (_, errorEntity) = distributedEntityProvisionService.distributeCreateEntityForContextSources(
            listOf(csr),
            RegistrationInfoFilter(),
            expandJsonLdEntity(entity),
            contexts
        )

        assertNull(errorEntity?.getAttributes()?.get(NGSILD_NAME_PROPERTY))
        assertNotNull(errorEntity?.getAttributes()?.get(TEMPERATURE_PROPERTY))
    }

    @Test
    fun `postDistributedInformation should process badly formed errors`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"

        stubFor(
            post(urlMatching(path))
                .willReturn(badRequest().withBody(invalidErrorResponse))
        )

        val entity = compactEntity(expandJsonLdEntity(entity), listOf(APIC_COMPOUND_CONTEXT))
        val response = distributedEntityProvisionService.postDistributedInformation(entity, csr, path)

        assertTrue(response.isLeft())
        assertEquals(response.leftOrNull()?.type, ErrorType.BAD_GATEWAY.type)
        assertEquals(response.leftOrNull()?.status, HttpStatus.BAD_GATEWAY)
        assertEquals(response.leftOrNull()?.detail, invalidErrorResponse)
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
        val response = distributedEntityProvisionService.postDistributedInformation(entity, csr, path)

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
        val response = distributedEntityProvisionService.postDistributedInformation(entity, csr, path)

        assertTrue(response.isLeft())
        assertInstanceOf(GatewayTimeoutException::class.java, response.leftOrNull())
    }
}
