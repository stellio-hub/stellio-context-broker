package com.egm.stellio.search.csr.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.entity.web.BatchEntitySuccess
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.ErrorType
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.ResourceNotFoundException
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
import io.mockk.coVerify
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
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.net.URI

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
    fun `distributeDeleteEntity should return the received errors`() = runTest {
        val firstExclusiveCsr = gimmeRawCSR(mode = Mode.EXCLUSIVE, operations = listOf(Operation.DELETE_ENTITY))
        val firstRedirectCsr = gimmeRawCSR(mode = Mode.REDIRECT, operations = listOf(Operation.REDIRECTION_OPS))
        val firstInclusiveCsr = gimmeRawCSR(mode = Mode.INCLUSIVE, operations = listOf(Operation.REDIRECTION_OPS))
        val secondRedirectCsr = gimmeRawCSR(mode = Mode.REDIRECT, operations = listOf(Operation.REDIRECTION_OPS))
        val secondInclusiveCsr = gimmeRawCSR(mode = Mode.INCLUSIVE, operations = listOf(Operation.CREATE_ENTITY))

        val requestParams = LinkedMultiValueMap<String, String>()

        val entityId = expandJsonLdEntity(entity).id
        val errorMessage = "test error"
        coEvery {
            distributedEntityProvisionService.sendDistributedInformation(any(), any(), any(), any(), any())
        } returns Unit.right() andThen
            Unit.right() andThen
            ResourceNotFoundException("first $errorMessage").left() andThen
            ResourceNotFoundException("second $errorMessage").left()

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(firstInclusiveCsr, firstExclusiveCsr, firstRedirectCsr, secondInclusiveCsr, secondRedirectCsr)

        val result = distributedEntityProvisionService.distributeDeleteEntity(
            entityId,
            requestParams
        )
        assertThat(result.success).hasSize(2)
        assertThat(result.errors).hasSize(2)
        assertThat(result.errors).anyMatch { it.error.title!!.contains(errorMessage) }

        coVerify(exactly = 4) {
            distributedEntityProvisionService.sendDistributedInformation(
                null,
                any(),
                any(),
                HttpMethod.DELETE,
                requestParams
            )
        }
    }

    @Test
    fun `distributeCreateEntityForContextSources  should return the remainingEntity`() = runTest {
        val firstExclusiveCsr = gimmeRawCSR(id = "id:exclusive:1".toUri(), mode = Mode.EXCLUSIVE)
        val firstRedirectCsr = gimmeRawCSR(id = "id:redirect:1".toUri(), mode = Mode.REDIRECT)
        val firstInclusiveCsr = gimmeRawCSR(id = "id:inclusive:1".toUri(), mode = Mode.INCLUSIVE)
        val secondRedirectCsr = gimmeRawCSR(id = "id:redirect:2".toUri(), mode = Mode.REDIRECT)
        val secondInclusiveCsr = gimmeRawCSR(id = "id:inclusive:2".toUri(), mode = Mode.INCLUSIVE)

        val entryEntity = expandJsonLdEntity(entity)
        val entityWithIgnoredTemperature = entryEntity.omitAttributes(setOf(TEMPERATURE_PROPERTY))
        val entityWithIgnoredTemperatureAndName = entityWithIgnoredTemperature
            .omitAttributes(setOf(NGSILD_NAME_PROPERTY))

        coEvery {
            distributedEntityProvisionService.distributeEntityProvisionForContextSources(
                any(), any(), any(), any(), any(), any()
            )
        } returns
            entityWithIgnoredTemperature andThen entityWithIgnoredTemperatureAndName andThen null

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(firstInclusiveCsr, firstExclusiveCsr, firstRedirectCsr, secondInclusiveCsr, secondRedirectCsr)

        val (_, remainingEntity) = distributedEntityProvisionService.distributeCreateEntity(
            entryEntity,
            contexts
        )

        assertEquals(entityWithIgnoredTemperatureAndName, remainingEntity)

        coVerifyOrder {
            distributedEntityProvisionService.distributeEntityProvisionForContextSources(
                listOf(firstExclusiveCsr),
                any(),
                entryEntity,
                any(),
                any(),
                any()
            )
            distributedEntityProvisionService.distributeEntityProvisionForContextSources(
                listOf(firstRedirectCsr, secondRedirectCsr),
                any(),
                entityWithIgnoredTemperature,
                any(),
                any(),
                any()
            )
            distributedEntityProvisionService.distributeEntityProvisionForContextSources(
                listOf(firstInclusiveCsr, secondInclusiveCsr),
                any(),
                entityWithIgnoredTemperatureAndName,
                any(),
                any(),
                any()
            )
        }
    }

    @Test
    fun `distributeCreateEntityForContextSources should update the result`() = runTest {
        val csr = spyk(gimmeRawCSR(operations = listOf(Operation.REDIRECTION_OPS)))
        val firstURI = URI("id:1")
        val secondURI = URI("id:2")
        coEvery {
            distributedEntityProvisionService.sendDistributedInformation(any(), any(), any(), any())
        } returns contextSourceException.left() andThen Unit.right()

        coEvery {
            csr.getAssociatedAttributes(any(), any())
        } returns setOf(NGSILD_NAME_PROPERTY) andThen setOf(TEMPERATURE_PROPERTY)
        coEvery {
            csr.id
        } returns firstURI andThen secondURI

        val result = BatchOperationResult()

        distributedEntityProvisionService.distributeEntityProvisionForContextSources(
            listOf(csr, csr),
            CSRFilters(),
            expandJsonLdEntity(entity),
            contexts,
            result,
            Operation.CREATE_ENTITY,
        )
        coVerify(exactly = 2) {
            distributedEntityProvisionService.sendDistributedInformation(any(), any(), any(), HttpMethod.POST)
        }

        assertThat(result.success).hasSize(1)
        assertThat(result.success).contains(BatchEntitySuccess(secondURI))
        assertThat(result.errors).hasSize(1)
        assertEquals(firstURI, result.errors.first().registrationId)
        assertEquals(contextSourceException.message, result.errors.first().error.title)
    }

    @Test
    fun `distributeCreateEntityForContextSources should return null if whole entity has been processed`() = runTest {
        val csr = spyk(gimmeRawCSR())
        coEvery {
            csr.getAssociatedAttributes(any(), any())
        } returns setOf(NGSILD_NAME_PROPERTY) andThen setOf(TEMPERATURE_PROPERTY)

        coEvery {
            distributedEntityProvisionService.sendDistributedInformation(any(), any(), any(), any())
        } returns contextSourceException.left() andThen Unit.right()

        val entity = distributedEntityProvisionService.distributeEntityProvisionForContextSources(
            listOf(csr, csr),
            CSRFilters(),
            expandJsonLdEntity(entity),
            contexts,
            BatchOperationResult(),
            Operation.CREATE_ENTITY
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
            distributedEntityProvisionService.sendDistributedInformation(any(), any(), any(), any())
        } returns Unit.right()

        val successEntity = distributedEntityProvisionService.distributeEntityProvisionForContextSources(
            listOf(csr),
            CSRFilters(),
            expandJsonLdEntity(entity),
            contexts,
            BatchOperationResult(),
            Operation.CREATE_ENTITY
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
            distributedEntityProvisionService.sendDistributedInformation(any(), any(), any(), any())
        } returns contextSourceException.left()

        val errorEntity = distributedEntityProvisionService.distributeEntityProvisionForContextSources(
            listOf(csr),
            CSRFilters(),
            expandJsonLdEntity(entity),
            contexts,
            BatchOperationResult(),
            Operation.CREATE_ENTITY
        )

        assertNull(errorEntity?.getAttributes()?.get(NGSILD_NAME_PROPERTY))
        assertNotNull(errorEntity?.getAttributes()?.get(TEMPERATURE_PROPERTY))
    }

    @Test
    fun `sendDistributedInformation should process badly formed errors`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"

        stubFor(
            post(urlMatching(path))
                .willReturn(badRequest().withBody(invalidErrorResponse))
        )

        val entity = compactEntity(expandJsonLdEntity(entity), listOf(APIC_COMPOUND_CONTEXT))
        val response = distributedEntityProvisionService.sendDistributedInformation(entity, csr, path, HttpMethod.POST)

        assertTrue(response.isLeft())
        assertEquals(response.leftOrNull()?.type, ErrorType.BAD_GATEWAY.type)
        assertEquals(response.leftOrNull()?.status, HttpStatus.BAD_GATEWAY)
        assertEquals(response.leftOrNull()?.detail, invalidErrorResponse)
    }

    @Test
    fun `sendDistributedInformation should return the received error`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"

        stubFor(
            post(urlMatching(path))
                .willReturn(badRequest().withBody(validErrorResponse))
        )

        val entity = compactEntity(expandJsonLdEntity(entity), listOf(APIC_COMPOUND_CONTEXT))
        val response = distributedEntityProvisionService.sendDistributedInformation(entity, csr, path, HttpMethod.POST)

        assertTrue(response.isLeft())
        assertInstanceOf(ContextSourceException::class.java, response.leftOrNull())
        assertEquals(ErrorType.BAD_REQUEST_DATA.type, response.leftOrNull()?.type)
        assertEquals(HttpStatus.BAD_REQUEST, response.leftOrNull()?.status)
        assertEquals("The detail of the valid Error", response.leftOrNull()?.detail)
        assertEquals("A valid error", response.leftOrNull()?.message)
    }

    @Test
    fun `sendDistributedInformation should return a GateWayTimeout error if it receives no answer`() = runTest {
        val csr = gimmeRawCSR().copy(endpoint = "http://localhost:invalid".toUri())
        val path = "/ngsi-ld/v1/entities"
        val entity = compactEntity(expandJsonLdEntity(entity), listOf(APIC_COMPOUND_CONTEXT))
        val response = distributedEntityProvisionService.sendDistributedInformation(entity, csr, path, HttpMethod.POST)

        assertTrue(response.isLeft())
        assertInstanceOf(GatewayTimeoutException::class.java, response.leftOrNull())
    }
}
