package com.egm.stellio.search.temporal.web

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.service.TemporalService.CreateOrUpdateResult
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.TooManyResultsException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import com.egm.stellio.shared.util.attributeOrInstanceNotFoundMessage
import com.egm.stellio.shared.util.entityNotFoundMessage
import com.egm.stellio.shared.util.loadAndExpandSampleData
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.mockkClass
import kotlinx.coroutines.test.runTest
import org.hamcrest.core.Is
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.context.ActiveProfiles
import org.springframework.web.reactive.function.BodyInserters
import java.time.ZonedDateTime

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class TemporalEntityHandlerTests : TemporalEntityHandlerTestCommon() {

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val attributeName = "speed"
    private val attributeInstanceId = "urn:ngsi-ld:Instance:01".toUri()

    @Test
    fun `create temporal entity should return a 201`() = runTest {
        val jsonLdFile = loadSampleData("temporal/beehive_create_temporal_entity.jsonld")
        val expandedEntity = JsonLdUtils.expandJsonLdEntity(jsonLdFile, APIC_COMPOUND_CONTEXTS)

        coEvery {
            temporalService.createOrUpdateTemporalEntity(any(), any())
        } returns CreateOrUpdateResult.CREATED.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/temporal/entities/$entityUri"))

        coVerify {
            temporalService.createOrUpdateTemporalEntity(
                eq(entityUri),
                eq(expandedEntity)
            )
        }
    }

    @Test
    fun `update temporal entity should return a 204`() = runTest {
        val jsonLdFile = loadSampleData("temporal/beehive_update_temporal_entity.jsonld")
        val expandedEntity = JsonLdUtils.expandJsonLdEntity(jsonLdFile, APIC_COMPOUND_CONTEXTS)

        coEvery {
            temporalService.createOrUpdateTemporalEntity(any(), any())
        } returns CreateOrUpdateResult.UPSERTED.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            temporalService.createOrUpdateTemporalEntity(
                eq(entityUri),
                eq(expandedEntity)
            )
        }
    }

    @Test
    fun `it should correctly handle an attribute with one instance`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_one_attribute_one_instance.jsonld")

        coEvery { temporalService.upsertAttributes(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            temporalService.upsertAttributes(
                eq(entityUri),
                match {
                    it.size == 1
                }
            )
        }
    }

    @Test
    fun `it should correctly handle an attribute with many instances`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_one_attribute_many_instances.jsonld")

        coEvery { temporalService.upsertAttributes(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            temporalService.upsertAttributes(
                eq(entityUri),
                match {
                    it.size == 1
                }
            )
        }
    }

    @Test
    fun `it should correctly handle many attributes with one instance`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_many_attributes_one_instance.jsonld")

        coEvery { temporalService.upsertAttributes(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            temporalService.upsertAttributes(
                eq(entityUri),
                match {
                    it.size == 2
                }
            )
        }
    }

    @Test
    fun `it should correctly handle many attributes with many instances`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_many_attributes_many_instances.jsonld")

        coEvery { temporalService.upsertAttributes(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            temporalService.upsertAttributes(
                eq(entityUri),
                match {
                    it.size == 2
                }
            )
        }
    }

    @Test
    fun `it should return a 400 if temporal entity fragment is badly formed`() {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue("""{ "id": "bad" }"""))
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Unable to expand input payload"
                }
                """
            )
    }

    @Test
    fun `it should return a 403 is user is not authorized to write on the entity`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_many_attributes_many_instances.jsonld")

        coEvery {
            temporalService.upsertAttributes(any(), any())
        } returns AccessDeniedException("User forbidden write access to entity $entityUri").left()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `it should raise a 400 if timerel is present without time query param`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timerel=before")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'timerel' and 'time' must be used in conjunction"
                }
                """
            )
    }

    @Test
    fun `it should raise a 400 if time is present without timerel query param`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timeAt=2020-10-29T18:00:00Z")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'timerel' and 'time' must be used in conjunction"
                }
                """
            )
    }

    @Test
    fun `it should give a 200 if no timerel and no time query params are in the request`() {
        val returnedExpandedEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns (returnedExpandedEntity to null).right()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `it should raise a 400 if timerel is between and no endTimeAt provided`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timerel=between&timeAt=2020-10-29T18:00:00Z")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'endTimeAt' request parameter is mandatory if 'timerel' is 'between'"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if time is not parsable`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timerel=before&timeAt=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'timeAt' parameter is not a valid date"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if timerel is not a valid value`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timerel=befor&timeAt=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'timerel' is not valid, it should be one of 'before', 'between', or 'after'"
                }
                """
            )
    }

    @Test
    fun `it should raise a 400 if timerel is between and endTimeAt is not parseable`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=endTime"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'endTimeAt' parameter is not a valid date"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if one of time bucket or aggregate is missing`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&aggrPeriodDuration=P1DT1H&options=aggregatedValues"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'aggrMethods' is mandatory if 'aggregatedValues' option is specified"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if aggregate function is unknown`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&options=aggregatedValues&aggrMethods=unknown"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'unknown' is not a recognized aggregation method for 'aggrMethods' parameter"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if aggrPeriodDuration is not in the correct format`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&options=aggregatedValues&aggrPeriodDuration=PXD3N"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'aggrMethods' is mandatory if 'aggregatedValues' option is specified"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if temporalValues and aggregatedValues exist in options query param`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&aggrPeriodDuration=P1D&" +
                    "options=aggregatedValues,temporalValues"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Found different temporal representations in options query parameter, only one can be provided"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if format query param has an invalid value`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&" +
                    "format=invalid"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/InvalidRequest",
                    "title": "'invalid' is not a valid temporal representation"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if options query param has an invalid value`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&" +
                    "options=invalidOptions"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/InvalidRequest",
                    "title": "'invalidOptions' is not a valid value for the options query parameter"
                } 
                """
            )
    }

    @Test
    fun `it should return a 404 if temporal entity attribute does not exist`() {
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns ResourceNotFoundException("Entity urn:ngsi-ld:BeeHive:TESTC was not found").left()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "${entityNotFoundMessage(entityUri.toString())}"
                }
                """
            )
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid`() {
        val returnedExpandedEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns (returnedExpandedEntity to null).right()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk

        coVerify {
            temporalQueryService.queryTemporalEntity(
                eq(entityUri),
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalEntitiesQuery.temporalQuery.timeAt!!.isEqual(
                            ZonedDateTime.parse("2019-10-17T07:31:39Z")
                        ) &&
                        temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.NORMALIZED &&
                        !temporalEntitiesQuery.withAudit
                }
            )
        }
        confirmVerified(temporalQueryService)
    }

    @Test
    fun `it should return an entity with two temporal properties evolution`() = runTest {
        mockWithIncomingAndOutgoingTemporalProperties(false)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.id").exists()
            .jsonPath("$.type").exists()
            .jsonPath("$.incoming.length()").isEqualTo(2)
            .jsonPath("$.outgoing.length()").isEqualTo(2)
            .jsonPath("$.@context").exists()
    }

    @Test
    fun `it should return a json entity with two temporal properties evolution`() = runTest {
        mockWithIncomingAndOutgoingTemporalProperties(false)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", APIC_HEADER_LINK)
            .header("Accept", MediaType.APPLICATION_JSON.toString())
            .exchange()
            .expectStatus().isOk
            .expectHeader().exists("Link")
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.id").exists()
            .jsonPath("$.type").exists()
            .jsonPath("$.incoming.length()").isEqualTo(2)
            .jsonPath("$.outgoing.length()").isEqualTo(2)
            .jsonPath("$.@context").doesNotExist()
    }

    @Test
    fun `it should return an entity with two temporal properties evolution with temporalValues option`() = runTest {
        mockWithIncomingAndOutgoingTemporalProperties(true)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&options=temporalValues"
            )
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.id").exists()
            .jsonPath("$.type").exists()
            .jsonPath("$..observedAt").doesNotExist()
            .jsonPath("$.incoming.values.length()").isEqualTo(2)
            .jsonPath("$.outgoing.values.length()").isEqualTo(2)
    }

    @Test
    fun `it should wrap single instance normalized attributes to a list of one instance`() = runTest {
        val temporalEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_one_evolution.jsonld")
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns (temporalEntity to null).right()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.incoming.length()").isEqualTo(1)
            .jsonPath("$.outgoing.length()").isEqualTo(1)
    }

    private suspend fun mockWithIncomingAndOutgoingTemporalProperties(withTemporalValues: Boolean) {
        val entityFileName = if (withTemporalValues)
            "beehive_with_two_temporal_attributes_evolution_temporal_values.jsonld"
        else
            "beehive_with_two_temporal_attributes_evolution.jsonld"

        val entityResponseWith2temporalEvolutions = loadAndExpandSampleData(entityFileName)
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns (entityResponseWith2temporalEvolutions to null).right()
    }

    @Test
    fun `it should raise a 400 and return an error response`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities?type=Beehive&timerel=before")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'timerel' and 'time' must be used in conjunction"
                }
                """
            )
    }

    @Test
    fun `it should return a 200 with empty payload if no temporal attribute is found`() {
        val temporalQuery = buildDefaultTestTemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(emptyList(), 2, null))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&type=BeeHive"
            )
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        coVerify {
            temporalQueryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    val entitiesQueryFromGet = temporalEntitiesQuery.entitiesQuery as EntitiesQueryFromGet
                    temporalEntitiesQuery.entitiesQuery.paginationQuery.limit == 30 &&
                        temporalEntitiesQuery.entitiesQuery.paginationQuery.offset == 0 &&
                        entitiesQueryFromGet.ids.isEmpty() &&
                        entitiesQueryFromGet.typeSelection == BEEHIVE_IRI &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.NORMALIZED
                }
            )
        }
    }

    @Test
    fun `it should return 200 with jsonld response body for query temporal entities`() = runTest {
        val firstTemporalEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(listOf(firstTemporalEntity, secondTemporalEntity), 2, null))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive"
            )
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$.length()").isEqualTo(2)
            .jsonPath("$[0].@context").exists()
            .jsonPath("$[1].@context").exists()
    }

    @Test
    fun `it should return 200 with json response body for query temporal entities`() = runTest {
        val firstTemporalEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(listOf(firstTemporalEntity, secondTemporalEntity), 2, null))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive"
            )
            .header("Accept", MediaType.APPLICATION_JSON.toString())
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectHeader().exists("Link")
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$.length()").isEqualTo(2)
            .jsonPath("$[0].@context").doesNotExist()
            .jsonPath("$[1].@context").doesNotExist()
    }

    @Test
    fun `query temporal entity should return 200 with prev link header if exists`() = runTest {
        val firstTemporalEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(listOf(firstTemporalEntity, secondTemporalEntity), 2, null))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=2"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader()
            .valueEquals(
                "Link",
                """
                    </ngsi-ld/v1/temporal/entities?timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&type=BeeHive&limit=1&offset=1>;rel="prev";type="application/ld+json"
                """.trimIndent()
            )
    }

    @Test
    fun `query temporal entity should return 200 and empty response if requested offset does not exist`() {
        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(emptyList(), 2, null))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=9"
            )
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `query temporal entities should return 200 and the number of results if count is asked for`() {
        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(emptyList(), 2, null))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=0&offset=9&count=true"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "2")
            .expectBody()
    }

    @Test
    fun `query temporal entity should return 200 with next link header if exists`() = runTest {
        val firstTemporalEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(listOf(firstTemporalEntity, secondTemporalEntity), 2, null))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=0"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader()
            .valueEquals(
                "Link",
                """
                    </ngsi-ld/v1/temporal/entities?timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&type=BeeHive&limit=1&offset=1>;rel="next";type="application/ld+json"
                """.trimIndent()
            )
    }

    @Test
    fun `query temporal entity should return 200 with prev and next link header if exists`() = runTest {
        val firstTemporalEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(listOf(firstTemporalEntity, secondTemporalEntity), 3, null))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=1"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader()
            .valueEquals(
                "Link",
                """
                    </ngsi-ld/v1/temporal/entities?timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&type=BeeHive&limit=1&offset=0>;rel="prev";type="application/ld+json"
                """.trimIndent(),
                """
                    </ngsi-ld/v1/temporal/entities?timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&type=BeeHive&limit=1&offset=2>;rel="next";type="application/ld+json"
                """.trimIndent()
            )
    }

    @Test
    fun `query temporal entity should return 400 if requested offset is less than zero`() {
        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } throws BadRequestDataException(
            "Offset must be greater than zero and limit must be strictly greater than zero"
        )

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=-1"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Offset must be greater than zero and limit must be strictly greater than zero"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `query temporal entity should return 400 if limit is equal or less than zero`() {
        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } throws BadRequestDataException(
            "Offset must be greater than zero and limit must be strictly greater than zero"
        )

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=-1&offset=1"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Offset must be greater than zero and limit must be strictly greater than zero"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `query temporal entity should return 403 if limit is greater than the maximum authorized limit`() {
        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } throws TooManyResultsException(
            "You asked for 200 results, but the supported maximum limit is 100"
        )

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=200&offset=1"
            )
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/TooManyResults",
                    "title": "You asked for 200 results, but the supported maximum limit is 100"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `modify attribute instance should return a 204 if an instance has been successfully modified`() {
        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")

        coEvery { temporalService.modifyAttributeInstance(any(), any(), any()) } returns Unit.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM/$attributeInstanceId")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(instanceTemporalFragment))
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            temporalService.modifyAttributeInstance(
                entityUri,
                attributeInstanceId,
                any()
            )
        }
    }

    @Test
    fun `modify attribute instance should return a 403 is user is not authorized to update an entity `() {
        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")

        coEvery {
            temporalService.modifyAttributeInstance(any(), any(), any())
        } returns AccessDeniedException("User forbidden write access to entity $entityUri").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM/$attributeInstanceId")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(instanceTemporalFragment))
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden write access to entity $entityUri"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `modify attribute instance should return a 404 if entity to be update has not been found`() {
        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")

        coEvery {
            temporalService.modifyAttributeInstance(any(), any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM/$attributeInstanceId")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(instanceTemporalFragment))
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "Entity urn:ngsi-ld:BeeHive:TESTC was not found"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `modify attribute instance should return a 404 if attributeInstanceId or attribute name is not found`() {
        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(attributeName, NGSILD_TEST_CORE_CONTEXT)

        coEvery {
            temporalService.modifyAttributeInstance(any(), any(), any())
        } returns ResourceNotFoundException(
            attributeOrInstanceNotFoundMessage(expandedAttr, attributeInstanceId.toString())
        ).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM/$attributeInstanceId")
            .header("Link", APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(instanceTemporalFragment))
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "${attributeOrInstanceNotFoundMessage(expandedAttr, attributeInstanceId.toString())}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete temporal entity should return a 204 if an entity has been successfully deleted`() {
        coEvery { temporalService.deleteEntity(any()) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            temporalService.deleteEntity(eq(entityUri))
        }
    }

    @Test
    fun `delete temporal entity should return a 404 if entity to be deleted has not been found`() {
        coEvery {
            temporalService.deleteEntity(entityUri)
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "${entityNotFoundMessage(entityUri.toString())}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete temporal entity should return a 400 if the provided id is not a valid URI`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/beehive")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "The supplied identifier was expected to be an URI but it is not: beehive"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete temporal entity should return a 405 if entity id is missing`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.METHOD_NOT_ALLOWED)
    }

    @Test
    fun `delete temporal entity should return a 500 if entity could not be deleted`() {
        coEvery {
            temporalService.deleteEntity(any())
        } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json(
                """
                    {
                        "type": "https://uri.etsi.org/ngsi-ld/errors/InternalError",
                        "title": "java.lang.RuntimeException: Unexpected server error",
  
                    }
                    """
            )
    }

    @Test
    fun `delete temporal entity should return a 403 is user is not authorized to delete an entity`() {
        coEvery {
            temporalService.deleteEntity(any())
        } returns AccessDeniedException("User forbidden admin access to entity $entityUri").left()

        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden admin access to entity $entityUri"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 204 if the attribute has been successfully deleted`() {
        coEvery {
            temporalService.deleteAttribute(any(), any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            temporalService.deleteAttribute(
                eq(entityUri),
                eq(TEMPERATURE_IRI),
                null,
                eq(false)
            )
        }
    }

    @Test
    fun `delete attribute temporal should delete all instances if deleteAll flag is true`() {
        coEvery {
            temporalService.deleteAttribute(any(), any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            temporalService.deleteAttribute(
                eq(entityUri),
                eq(TEMPERATURE_IRI),
                null,
                eq(true)
            )
        }
    }

    @Test
    fun `delete attribute temporal should delete instance with the provided datasetId`() {
        val datasetId = "urn:ngsi-ld:Dataset:temperature:1"
        coEvery {
            temporalService.deleteAttribute(any(), any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM?datasetId=$datasetId")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            temporalService.deleteAttribute(
                eq(entityUri),
                eq(TEMPERATURE_IRI),
                eq(datasetId.toUri()),
                eq(false)
            )
        }
    }

    @Test
    fun `delete attribute temporal should return a 404 if the entity is not found`() {
        coEvery {
            temporalService.deleteAttribute(any(), any(), any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "Entity urn:ngsi-ld:BeeHive:TESTC was not found"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 404 if the attribute is not found`() {
        coEvery {
            temporalService.deleteAttribute(any(), any(), any(), any())
        } returns ResourceNotFoundException("Attribute Not Found").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "Attribute Not Found"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 400 if the request is not correct`() {
        coEvery {
            temporalService.deleteAttribute(any(), any(), any(), any())
        } returns BadRequestDataException("Something is wrong with the request").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.BAD_REQUEST)
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Something is wrong with the request"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 400 if the provided id is not a valid URI`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/beehive/attrs/$TEMPERATURE_TERM")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "The supplied identifier was expected to be an URI but it is not: beehive"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 400 if entity id is missing`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities//attrs/$TEMPERATURE_TERM")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Missing entity id or attribute id when trying to delete an attribute temporal"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 400 if attribute name is missing`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Missing entity id or attribute id when trying to delete an attribute temporal"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 403 if user is not allowed to update entity`() {
        coEvery {
            temporalService.deleteAttribute(any(), any(), any(), any())
        } returns AccessDeniedException("User forbidden write access to entity $entityUri").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden write access to entity urn:ngsi-ld:BeeHive:TESTC"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute instance temporal should return 204`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(attributeName, NGSILD_TEST_CORE_CONTEXT)
        coEvery {
            temporalService.deleteAttributeInstance(any(), any(), any())
        } returns Unit.right()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$attributeName/$attributeInstanceId")
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            temporalService.deleteAttributeInstance(entityUri, expandedAttr, attributeInstanceId)
        }
    }

    @Test
    fun `delete attribute instance temporal should return 404 if entityId is not found`() {
        coEvery {
            temporalService.deleteAttributeInstance(any(), any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$attributeName/$attributeInstanceId")
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "${entityNotFoundMessage(entityUri.toString())}"
                }
                """.trimIndent()
            )

        coVerify {
            temporalService.deleteAttributeInstance(
                entityUri,
                NGSILD_DEFAULT_VOCAB + attributeName,
                attributeInstanceId
            )
        }
    }

    @Test
    fun `delete attribute instance temporal should return 404 if attributeInstanceId or attribute name is not found`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(attributeName, NGSILD_TEST_CORE_CONTEXT)

        coEvery {
            temporalService.deleteAttributeInstance(any(), any(), any())
        } returns ResourceNotFoundException(
            attributeOrInstanceNotFoundMessage(expandedAttr, attributeInstanceId.toString())
        ).left()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$attributeName/$attributeInstanceId")
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "${attributeOrInstanceNotFoundMessage(expandedAttr, attributeInstanceId.toString())}"
                }
                """.trimIndent()
            )

        coVerify {
            temporalService.deleteAttributeInstance(entityUri, expandedAttr, attributeInstanceId)
        }
    }

    @Test
    fun `delete attribute instance temporal should return 403 if user is not allowed`() {
        coEvery {
            temporalService.deleteAttributeInstance(any(), any(), any())
        } returns AccessDeniedException("User forbidden write access to entity $entityUri").left()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$attributeName/$attributeInstanceId")
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden write access to entity $entityUri"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `query temporal entity should return 400 if neither type nor attrs is present`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "One of 'type', 'attrs', 'q', 'geoQ' must be provided in the query unless local is true"
                }
                """.trimIndent()
            )
    }
}
