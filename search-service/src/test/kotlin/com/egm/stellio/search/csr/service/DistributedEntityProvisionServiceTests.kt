package com.egm.stellio.search.csr.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromGet
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.buildDefaultPagination
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.ErrorType
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.NGSILD_ALL_ENTITIES
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.toTypeSelection
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.client.WireMock.badRequest
import com.github.tomakehurst.wiremock.client.WireMock.delete
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.status
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.MockkSpyBean
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
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.net.URI

@SpringBootTest
@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class DistributedEntityProvisionServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @MockkSpyBean
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
    fun `distributeCreateEntity should call distributeEntityProvision`() = runTest {
        val entity = expandJsonLdEntity(entity)

        coEvery {
            distributedEntityProvisionService.distributeEntityProvision(any(), any(), any(), any())
        } returns (BatchOperationResult() to entity)

        distributedEntityProvisionService.distributeCreateEntity(
            entity,
            contexts
        )

        coVerify {
            distributedEntityProvisionService.distributeEntityProvision(
                entity,
                contexts,
                Operation.CREATE_ENTITY,
                entity.types.toTypeSelection()
            )
        }
    }

    @Test
    fun `distributeReplaceEntity should call distributeEntityProvision`() = runTest {
        val entity = expandJsonLdEntity(entity)

        coEvery {
            distributedEntityProvisionService.distributeEntityProvision(any(), any(), any(), any())
        } returns (BatchOperationResult() to entity)

        distributedEntityProvisionService.distributeReplaceEntity(
            entity,
            contexts,
            LinkedMultiValueMap()
        )

        coVerify {
            distributedEntityProvisionService.distributeEntityProvision(
                entity,
                contexts,
                Operation.REPLACE_ENTITY,
                entity.types.toTypeSelection()
            )
        }
    }

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
    fun `distributeEntityProvisionForContextSources should return the remainingEntity`() = runTest {
        val firstExclusiveCsr = gimmeRawCSR(id = "id:exclusive:1".toUri(), mode = Mode.EXCLUSIVE)
        val firstRedirectCsr = gimmeRawCSR(id = "id:redirect:1".toUri(), mode = Mode.REDIRECT)
        val firstInclusiveCsr = gimmeRawCSR(id = "id:inclusive:1".toUri(), mode = Mode.INCLUSIVE)
        val secondRedirectCsr = gimmeRawCSR(id = "id:redirect:2".toUri(), mode = Mode.REDIRECT)
        val secondInclusiveCsr = gimmeRawCSR(id = "id:inclusive:2".toUri(), mode = Mode.INCLUSIVE)

        val entryEntity = expandJsonLdEntity(entity)
        val entityWithIgnoredTemperature = entryEntity.omitAttributes(setOf(TEMPERATURE_IRI))
        val entityWithIgnoredTemperatureAndName = entityWithIgnoredTemperature
            .omitAttributes(setOf(NAME_IRI))

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
    fun `distributeEntityProvisionForContextSources should update the result`() = runTest {
        val csr = spyk(gimmeRawCSR(operations = listOf(Operation.REDIRECTION_OPS)))
        val firstURI = URI("id:1")
        val secondURI = URI("id:2")
        coEvery {
            distributedEntityProvisionService.sendDistributedInformation(any(), any(), any(), any())
        } returns contextSourceException.left() andThen Unit.right()

        coEvery {
            csr.getAttributesMatchingCSFAndEntity(any(), any())
        } returns setOf(NAME_IRI) andThen setOf(TEMPERATURE_IRI)
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
        assertThat(result.success).contains(secondURI)
        assertThat(result.errors).hasSize(1)
        assertEquals(firstURI, result.errors.first().registrationId)
        assertEquals(contextSourceException.message, result.errors.first().error.title)
    }

    @Test
    fun `distributeEntityProvisionForContextSources should return null if whole entity has been processed`() = runTest {
        val csr = spyk(gimmeRawCSR())
        coEvery {
            csr.getAttributesMatchingCSFAndEntity(any(), any())
        } returns setOf(NAME_IRI) andThen setOf(TEMPERATURE_IRI)

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
    fun `distributeEntityProvisionForContextSources should return only non processed attributes`() = runTest {
        val csr = spyk(gimmeRawCSR())
        coEvery {
            csr.getAttributesMatchingCSFAndEntity(any(), any())
        } returns setOf(NAME_IRI)

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

        assertNull(successEntity?.getAttributes()?.get(NAME_IRI))
        assertNotNull(successEntity?.getAttributes()?.get(TEMPERATURE_IRI))
    }

    @Test
    fun `distributeEntityProvisionForContextSources should remove the attrs even when the csr is in error`() = runTest {
        val csr = spyk(gimmeRawCSR())

        coEvery {
            csr.getAttributesMatchingCSFAndEntity(any(), any())
        } returns setOf(NAME_IRI)
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

        assertNull(errorEntity?.getAttributes()?.get(NAME_IRI))
        assertNotNull(errorEntity?.getAttributes()?.get(TEMPERATURE_IRI))
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

    @Test
    fun `distributePurgeEntities should call sendDistributedPurgeOperation`() = runTest {
        val csrWithEntityInfoId = gimmeRawCSR(
            information = listOf(
                RegistrationInfo(listOf(EntityInfo(id = apiaryId.toUri(), types = listOf(APIARY_IRI))))
            ),
            operations = listOf(Operation.PURGE_ENTITY)
        )

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csrWithEntityInfoId)

        coEvery {
            distributedEntityProvisionService.sendDistributedPurgeOperation(any(), any(), any())
        } returns Pair(null, HttpStatusCode.valueOf(204)).right()

        val queryParams = LinkedMultiValueMap<String, String>()
        val result = distributedEntityProvisionService.distributePurgeEntities(
            HttpHeaders.EMPTY,
            composeEntitiesQueryFromGet(buildDefaultPagination(), queryParams, emptyList()).getOrNull()!!,
            queryParams
        )

        assertTrue(result.isRight())

        coVerify(exactly = 1) {
            distributedEntityProvisionService.sendDistributedPurgeOperation(
                HttpHeaders.EMPTY,
                csrWithEntityInfoId,
                match { params ->
                    params.getFirst(QP.ID.key) == apiaryId &&
                        params.getFirst(QP.TYPE.key) == APIARY_IRI
                }
            )
        }
    }

    @Test
    fun `distributePurgeEntities should process keep attribute before calling sendDistributedPurgeOperation`() =
        runTest {
            val csrWithEntityInfoId = gimmeRawCSR(
                information = listOf(
                    RegistrationInfo(
                        listOf(EntityInfo(id = apiaryId.toUri(), types = listOf(APIARY_IRI))),
                        listOf("prop1", "prop2"),
                        listOf("rel1")
                    )
                ),
                operations = listOf(Operation.PURGE_ENTITY)
            )

            coEvery {
                contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
            } returns listOf(csrWithEntityInfoId)

            coEvery {
                distributedEntityProvisionService.sendDistributedPurgeOperation(any(), any(), any())
            } returns Pair(null, HttpStatusCode.valueOf(204)).right()

            val queryParams = LinkedMultiValueMap(
                mapOf(QP.KEEP.key to listOf("prop1,rel1"))
            )
            val result = distributedEntityProvisionService.distributePurgeEntities(
                HttpHeaders.EMPTY,
                composeEntitiesQueryFromGet(buildDefaultPagination(), queryParams, emptyList()).getOrNull()!!,
                queryParams
            )

            assertTrue(result.isRight())

            coVerify(exactly = 1) {
                distributedEntityProvisionService.sendDistributedPurgeOperation(
                    HttpHeaders.EMPTY,
                    csrWithEntityInfoId,
                    match { params ->
                        params.getFirst(QP.DROP.key) == "prop2" &&
                            params.getFirst(QP.KEEP.key) == null
                    }
                )
            }
        }

    @Test
    fun `distributePurgeEntities should process drop attribute before calling sendDistributedPurgeOperation`() =
        runTest {
            val csrWithEntityInfoId = gimmeRawCSR(
                information = listOf(
                    RegistrationInfo(
                        listOf(EntityInfo(id = apiaryId.toUri(), types = listOf(APIARY_IRI))),
                        listOf("prop1", "prop2"),
                        listOf("rel1")
                    )
                ),
                operations = listOf(Operation.PURGE_ENTITY)
            )

            coEvery {
                contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
            } returns listOf(csrWithEntityInfoId)

            coEvery {
                distributedEntityProvisionService.sendDistributedPurgeOperation(any(), any(), any())
            } returns Pair(null, HttpStatusCode.valueOf(204)).right()

            val queryParams = LinkedMultiValueMap(
                mapOf(QP.DROP.key to listOf("prop1,rel2"))
            )
            val result = distributedEntityProvisionService.distributePurgeEntities(
                HttpHeaders.EMPTY,
                composeEntitiesQueryFromGet(buildDefaultPagination(), queryParams, emptyList()).getOrNull()!!,
                queryParams
            )

            assertTrue(result.isRight())

            coVerify(exactly = 1) {
                distributedEntityProvisionService.sendDistributedPurgeOperation(
                    HttpHeaders.EMPTY,
                    csrWithEntityInfoId,
                    match { params ->
                        params.getFirst(QP.DROP.key) == "prop1"
                    }
                )
            }
        }

    @Test
    fun `distributePurgeEntities should return an error when both request and CSR define idPattern`() = runTest {
        val csrWithIdPattern = gimmeRawCSR(
            information = listOf(
                RegistrationInfo(
                    listOf(EntityInfo(idPattern = "urn:ngsi-ld:Apiary:.*", types = listOf(APIARY_IRI)))
                )
            ),
            operations = listOf(Operation.PURGE_ENTITY)
        )

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csrWithIdPattern)

        val queryParams = LinkedMultiValueMap(
            mapOf(QP.ID_PATTERN.key to listOf("urn:ngsi-ld:.*"))
        )

        val result = distributedEntityProvisionService.distributePurgeEntities(
            HttpHeaders.EMPTY,
            composeEntitiesQueryFromGet(buildDefaultPagination(), queryParams, emptyList()).getOrNull()!!,
            queryParams
        )

        assertTrue(result.isRight())
        assertThat(result.getOrNull())
            .isNotNull()
            .matches {
                it?.errors?.size == 1 &&
                    it.success.isEmpty() &&
                    it.errors[0].error.type == ErrorType.CONFLICT.type
            }
    }

    @Test
    fun `distributePurgeEntities should handle a 400 returned by the CSR`() = runTest {
        val csr = gimmeRawCSR(operations = listOf(Operation.PURGE_ENTITY))

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csr)

        coEvery {
            distributedEntityProvisionService.sendDistributedPurgeOperation(any(), any(), any())
        } returns BadGatewayException("Bad gateway").left()

        val queryParams = LinkedMultiValueMap(
            mapOf(QP.TYPE.key to listOf(APIARY_IRI))
        )

        val result = distributedEntityProvisionService.distributePurgeEntities(
            HttpHeaders.EMPTY,
            composeEntitiesQueryFromGet(buildDefaultPagination(), queryParams, emptyList()).getOrNull()!!,
            queryParams
        )

        assertTrue(result.isRight())
        assertEquals(1, result.getOrNull()?.errors?.size)
        assertThat(result.getOrNull()?.errors?.first())
            .extracting("entityId", "error.type", "error.status", "error.title", "registrationId")
            .containsExactly(
                NGSILD_ALL_ENTITIES,
                ErrorType.BAD_GATEWAY.type,
                HttpStatus.BAD_GATEWAY.value(),
                "Bad gateway",
                csr.id
            )
    }

    @Test
    fun `distributePurgeEntities should handle a 207 returned by the CSR`() = runTest {
        val csr = gimmeRawCSR(operations = listOf(Operation.PURGE_ENTITY))

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csr)

        coEvery {
            distributedEntityProvisionService.sendDistributedPurgeOperation(any(), any(), any())
        } returns Pair(
            """
            {
                "success": [],
                "errors": [
                    {
                        "entityId": "urn:ngsi-ld:Apiary:3",
                        "error": {
                            "type": "https://uri.etsi.org/ngsi-ld/errors/NotImplemented",
                            "status": 501,
                            "title": "Query param not implemented for this operation"
                        },
                        "registrationId": "${csr.id}"
                    }
                ]
            }
            """.trimIndent(),
            HttpStatus.MULTI_STATUS
        ).right()

        val queryParams = LinkedMultiValueMap(
            mapOf(QP.TYPE.key to listOf(APIARY_IRI))
        )

        val result = distributedEntityProvisionService.distributePurgeEntities(
            HttpHeaders.EMPTY,
            composeEntitiesQueryFromGet(buildDefaultPagination(), queryParams, emptyList()).getOrNull()!!,
            queryParams
        )

        assertTrue(result.isRight())
        assertEquals(1, result.getOrNull()?.errors?.size)
        assertThat(result.getOrNull()?.errors?.first())
            .extracting("entityId", "error.type", "error.status", "error.title", "registrationId")
            .containsExactly(
                "urn:ngsi-ld:Apiary:3".toUri(),
                ErrorType.NOT_IMPLEMENTED.type,
                HttpStatus.NOT_IMPLEMENTED.value(),
                "Query param not implemented for this operation",
                csr.id
            )
    }

    @Test
    fun `distributePurgeEntities should handle a 207 containing success and errors returned by the CSR`() = runTest {
        val csr = gimmeRawCSR(operations = listOf(Operation.PURGE_ENTITY))

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(csr)

        coEvery {
            distributedEntityProvisionService.sendDistributedPurgeOperation(any(), any(), any())
        } returns Pair(
            """
            {
                "success": ["urn:ngsi-ld:Apiary:1", "urn:ngsi-ld:Apiary:2"],
                "errors": [
                    {
                        "entityId": "urn:ngsi-ld:Apiary:3",
                        "error": {
                            "type": "https://uri.etsi.org/ngsi-ld/errors/InternalError",
                            "status": 500,
                            "title": "An unexpected error occurred"
                        },
                        "registrationId": "${csr.id}"
                    }
                ]
            }
            """.trimIndent(),
            HttpStatus.MULTI_STATUS
        ).right()

        val queryParams = LinkedMultiValueMap(
            mapOf(QP.TYPE.key to listOf(APIARY_IRI))
        )

        val result = distributedEntityProvisionService.distributePurgeEntities(
            HttpHeaders.EMPTY,
            composeEntitiesQueryFromGet(buildDefaultPagination(), queryParams, emptyList()).getOrNull()!!,
            queryParams
        )

        assertTrue(result.isRight())
        assertEquals(1, result.getOrNull()?.errors?.size)
        assertThat(result.getOrNull()?.errors?.first())
            .extracting("entityId", "error.type", "error.status", "error.title", "registrationId")
            .containsExactly(
                "urn:ngsi-ld:Apiary:3".toUri(),
                ErrorType.INTERNAL_ERROR.type,
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                "An unexpected error occurred",
                csr.id
            )
        assertEquals(2, result.getOrNull()?.success?.size)
        assertThat(result.getOrNull()?.success!!)
            .isEqualTo(listOf("urn:ngsi-ld:Apiary:1".toUri(), "urn:ngsi-ld:Apiary:2".toUri()))
    }

    @Test
    fun `distributePurgeEntities should handle multiple 207 returned by CSRs`() = runTest {
        val firstCsr = gimmeRawCSR(
            id = "id:exclusive:1".toUri(),
            operations = listOf(Operation.PURGE_ENTITY),
            mode = Mode.EXCLUSIVE
        )
        val secondCsr = gimmeRawCSR(
            id = "id:redirect:1".toUri(),
            operations = listOf(Operation.PURGE_ENTITY),
            mode = Mode.REDIRECT
        )
        val csrBatchOperationResult = """
        {
            "success": [],
            "errors": [
                {
                    "entityId": "urn:ngsi-ld:Apiary:3",
                    "error": {
                        "type": "https://uri.etsi.org/ngsi-ld/errors/NotImplemented",
                        "status": 501,
                        "title": "Query param not implemented for this operation"
                    },
                    "registrationId": "${firstCsr.id}"
                }
            ]
        }
        """.trimIndent()

        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(firstCsr, secondCsr)

        coEvery {
            distributedEntityProvisionService.sendDistributedPurgeOperation(any(), any(), any())
        } returns Pair(
            csrBatchOperationResult,
            HttpStatus.MULTI_STATUS
        ).right() andThen Pair(
            csrBatchOperationResult,
            HttpStatus.MULTI_STATUS
        ).right()

        val queryParams = LinkedMultiValueMap(
            mapOf(QP.TYPE.key to listOf(APIARY_IRI))
        )

        val result = distributedEntityProvisionService.distributePurgeEntities(
            HttpHeaders.EMPTY,
            composeEntitiesQueryFromGet(buildDefaultPagination(), queryParams, emptyList()).getOrNull()!!,
            queryParams
        )

        assertTrue(result.isRight())
        assertEquals(2, result.getOrNull()?.errors?.size)
    }

    @Test
    fun `sendDistributedPurgeOperation should process badly formed errors`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"

        stubFor(
            delete(urlMatching(path))
                .willReturn(badRequest().withBody(invalidErrorResponse))
        )

        val response = distributedEntityProvisionService.sendDistributedPurgeOperation(
            HttpHeaders.EMPTY,
            csr,
            LinkedMultiValueMap()
        )

        assertTrue(response.isLeft())
        assertEquals(response.leftOrNull()?.type, ErrorType.BAD_GATEWAY.type)
        assertEquals(response.leftOrNull()?.status, HttpStatus.BAD_GATEWAY)
        assertEquals(response.leftOrNull()?.detail, invalidErrorResponse)
    }

    @Test
    fun `sendDistributedPurgeOperation should return the received error`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"

        stubFor(
            delete(urlMatching(path))
                .willReturn(badRequest().withBody(validErrorResponse))
        )

        val response = distributedEntityProvisionService.sendDistributedPurgeOperation(
            HttpHeaders.EMPTY,
            csr,
            LinkedMultiValueMap()
        )

        assertTrue(response.isLeft())
        assertInstanceOf(ContextSourceException::class.java, response.leftOrNull())
        assertEquals(ErrorType.BAD_REQUEST_DATA.type, response.leftOrNull()?.type)
        assertEquals(HttpStatus.BAD_REQUEST, response.leftOrNull()?.status)
        assertEquals("The detail of the valid Error", response.leftOrNull()?.detail)
        assertEquals("A valid error", response.leftOrNull()?.message)
    }

    @Test
    fun `sendDistributedPurgeOperation should return a GatewayTimeout error if it receives no answer`() = runTest {
        val csr = gimmeRawCSR().copy(endpoint = "http://localhost:invalid".toUri())
        val response = distributedEntityProvisionService.sendDistributedPurgeOperation(
            HttpHeaders.EMPTY,
            csr,
            LinkedMultiValueMap()
        )

        assertTrue(response.isLeft())
        assertInstanceOf(GatewayTimeoutException::class.java, response.leftOrNull())
    }

    @Test
    fun `sendDistributedPurgeOperation should return the received 207 response`() = runTest {
        val csr = gimmeRawCSR()
        val path = "/ngsi-ld/v1/entities"
        val multiStatusResponse =
            """
            {
                "success": [],
                "errors": [
                {
                    "entityId": "urn:ngsi-ld:Apiary:3",
                    "error": {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/NotImplemented",
                    "status": 501,
                    "title": "Query param not implemented for this operation"
                },
                    "registrationId": "urn:ngsi-ld:ContextSourceRegistration:1"
                }
                ]
            }
            """.trimIndent()

        stubFor(
            delete(urlMatching(path))
                .willReturn(status(207).withBody(multiStatusResponse))
        )

        val response = distributedEntityProvisionService.sendDistributedPurgeOperation(
            HttpHeaders.EMPTY,
            csr,
            LinkedMultiValueMap()
        )

        assertTrue(response.isRight())
        val (body, status) = response.getOrNull()!!
        assertEquals(207, status.value())
        assertEquals(
            multiStatusResponse,
            body
        )
    }
}
