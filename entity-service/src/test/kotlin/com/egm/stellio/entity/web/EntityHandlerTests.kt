package com.egm.stellio.entity.web

import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_TIME_TYPE
import com.github.jsonldjava.core.JsonLdError
import com.github.jsonldjava.core.JsonLdError.Error.LOADING_REMOTE_CONTEXT_FAILED
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.hamcrest.core.Is
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.security.test.context.support.WithMockUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.lang.RuntimeException
import java.time.*

@ActiveProfiles("test")
@WebFluxTest(EntityHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockUser
class EntityHandlerTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityService: EntityService

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @Test
    fun `create entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214"

        every { entityService.exists(any()) } returns false
        every { entityService.createEntity(any()) } returns Entity(id = breedingServiceId, type = listOf("BeeHive"))

        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/$breedingServiceId"))
    }

    @Test
    fun `create entity should return a 409 if the entity already exists`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        every { entityService.exists(any()) } returns true

        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/AlreadyExists\"," +
                    "\"title\":\"The referred element already exists\"," +
                    "\"detail\":\"Already Exists\"}")
    }

    @Test
    fun `create entity should return a 500 error if internal server Error`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        every { entityService.exists(any()) } returns false
        every { entityService.createEntity(any()) } throws InternalErrorException("Internal Server Exception")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(500)
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                "\"title\":\"There has been an error during the operation execution\"," +
                "\"detail\":\"Internal Server Exception\"}")
    }

    @Test
    fun `create entity should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `create entity should return a 400 if Link header NS does not match the entity type`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `create entity should return a 400 if entity does not have an id`() {
        val entityWithoutId = """
            {
                "type": "Beehive"
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(entityWithoutId)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `create entity should return a 400 if entity does not have an type`() {
        val entityWithoutType = """
            {
                "id": "urn:ngsi-ld:Beehive:9876"
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(entityWithoutType)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `get entity by id should return 200 when entity exists`() {

        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns mockkClass(ExpandedEntity::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `get entity by id should correctly serialize temporal properties`() {
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns ExpandedEntity(
            mapOf(NGSILD_CREATED_AT_PROPERTY to
                    mapOf("@type" to NGSILD_DATE_TIME_TYPE,
                        "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)),
                "@id" to "urn:ngsi-ld:Beehive:4567",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\"createdAt\":\"2015-10-18T11:20:30.000001Z\",\"@context\":\"$NGSILD_CORE_CONTEXT\"}")
    }

    @Test
    fun `get entity by id should correctly serialize properties of type DateTime`() {

        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns ExpandedEntity(
            mapOf("https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf("@type" to "https://uri.etsi.org/ngsi-ld/Property",
                NGSILD_PROPERTY_VALUE to mapOf(
                    "@type" to NGSILD_DATE_TIME_TYPE,
                    "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC))),
                "@id" to "urn:ngsi-ld:Beehive:4567",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\"testedAt\":{\"type\":\"Property\",\"value\":{\"type\":\"DateTime\",\"@value\":\"2015-10-18T11:20:30.000001Z\"}},\"@context\":\"$NGSILD_CORE_CONTEXT\"}")
    }

    @Test
    fun `get entity by id should correctly serialize properties of type Date`() {

        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns ExpandedEntity(
            mapOf("https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf("@type" to "https://uri.etsi.org/ngsi-ld/Property",
                NGSILD_PROPERTY_VALUE to mapOf(
                    "@type" to NGSILD_DATE_TYPE,
                    "@value" to LocalDate.of(2015, 10, 18))),
                "@id" to "urn:ngsi-ld:Beehive:4567",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\"testedAt\":{\"type\":\"Property\",\"value\":{\"type\":\"Date\",\"@value\":\"2015-10-18\"}},\"@context\":\"$NGSILD_CORE_CONTEXT\"}")
    }

    @Test
    fun `get entity by id should correctly serialize properties of type Time`() {

        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns ExpandedEntity(
            mapOf("https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf("@type" to "https://uri.etsi.org/ngsi-ld/Property",
                NGSILD_PROPERTY_VALUE to mapOf(
                    "@type" to NGSILD_TIME_TYPE,
                    "@value" to LocalTime.of(11, 20, 30))),
                "@id" to "urn:ngsi-ld:Beehive:4567",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\"testedAt\":{\"type\":\"Property\",\"value\":{\"type\":\"Time\",\"@value\":\"11:20:30\"}},\"@context\":\"$NGSILD_CORE_CONTEXT\"}")
    }

    @Test
    fun `get entity by id should return 404 when entity does not exist`() {

        every { entityService.exists(any()) } returns false

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                "\"title\":\"The referred resource has not been found\"," +
                "\"detail\":\"Entity Not Found\"}")
    }

    @Test
    fun `search on entities should return 400 if required parameters are missing`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'q' or 'type' request parameters have to be specified (TEMP - cf 6.4.3.2\"}")
    }

    @Test
    fun `append entity attribute should return a 204 if all attributes were appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { entityService.exists(any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } returns UpdateResult(listOf("fishNumber"), emptyList())

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }
        verify { entityService.appendEntityAttributes(eq("urn:ngsi-ld:BreedingService:0214"), any(), eq(false)) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 207 if some attributes could not be appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { entityService.exists(any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) }
            .returns(UpdateResult(listOf("fishNumber"), listOf(NotUpdatedDetails("wrongAttribute", "overwrite disallowed"))))

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json("{\"updated\":[\"fishNumber\"],\"notUpdated\":[{\"attributeName\":\"wrongAttribute\",\"reason\":\"overwrite disallowed\"}]}")

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }
        verify { entityService.appendEntityAttributes(eq("urn:ngsi-ld:BreedingService:0214"), any(), eq(false)) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { entityService.exists(any()) } returns false

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity urn:ngsi-ld:BreedingService:0214 does not exist\"}")

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 400 if the attribute is missing a type`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty_missing_type.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { entityService.exists(any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } throws BadRequestDataException("@type not found")

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"@type not found\"}")

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 400 if the attribute is a property and is missing a value`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty_missing_value.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { entityService.exists(any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } throws BadRequestDataException("Key $NGSILD_PROPERTY_VALUE not found")

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"Key https://uri.etsi.org/ngsi-ld/hasValue not found\"}")

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 400 if the attribute is a relationship and is missing an object`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newRelationship_missing_object.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { entityService.exists(any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } throws BadRequestDataException("Key $NGSILD_RELATIONSHIP_HAS_OBJECT not found")

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"Key https://uri.etsi.org/ngsi-ld/hasObject not found\"}")

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }

        confirmVerified()
    }

    @Test
    fun `partial attribute update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN"
        val attrId = "fishNumber"

        every { entityService.updateEntityAttribute(any(), any(), any(), any()) } returns 1

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { entityService.updateEntityAttribute(eq(entityId), eq(attrId), any(), eq(aquacContext!!)) }
        confirmVerified(entityService)
    }

    @Test
    fun `entity attributes update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN"

        every { entityService.exists(any()) } returns true
        every { entityService.updateEntityAttributes(any(), any(), any()) } returns UpdateResult(updated = arrayListOf("fishNumber"), notUpdated = arrayListOf())

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { entityService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { entityService.updateEntityAttributes(eq(entityId), any(), eq(aquacContext!!)) }
        confirmVerified(entityService)
    }

    @Test
    fun `entity attributes update should return a 207 if some attributes are not found`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate_attributeNotFound.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN"
        val notUpdatedAttribute = NotUpdatedDetails("unknownAttribute", "Property Not Found")
        every { entityService.exists(any()) } returns true
        every { entityService.updateEntityAttributes(any(), any(), any()) } returns UpdateResult(updated = arrayListOf("fishNumber"), notUpdated = arrayListOf(notUpdatedAttribute))

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(207)

        verify { entityService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { entityService.updateEntityAttributes(eq(entityId), any(), eq(aquacContext!!)) }
        confirmVerified(entityService)
    }

    @Test
    fun `entity attributes update should return a 207 if some relationships objects are not found`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate_relationshipObjectNotFound.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN"
        val notUpdatedAttribute = NotUpdatedDetails("removedFrom",
            "Target entity unknownObject in property does not exist, create it first")
        every { entityService.exists(any()) } returns true
        every { entityService.updateEntityAttributes(any(), any(), any()) } returns UpdateResult(updated = arrayListOf("fishNumber"), notUpdated = arrayListOf(notUpdatedAttribute))

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)

        verify { entityService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { entityService.updateEntityAttributes(eq(entityId), any(), eq(aquacContext!!)) }
        confirmVerified(entityService)
    }

    @Test
    fun `entity attributes update should return a 400 if JSON-LD context is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/sensor_update.json")
        val entityId = "urn:ngsi-ld:Sensor:0022CCC"

        every { entityService.exists(any()) } returns true
        every { entityService.updateEntityAttributes(any(), any(), any()) } throws JsonLdError(LOADING_REMOTE_CONTEXT_FAILED, "http://easyglobalmarket.com/contexts/diat.jsonld")

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                "\"title\":\"loading remote context failed\"," +
                "\"detail\":\"loading remote context failed: http://easyglobalmarket.com/contexts/diat.jsonld\"}")
    }

    @Test
    fun `entity attributes update should return a 404 if entity type is unknown`() {
        val jsonLdFile = ClassPathResource("/ngsild/sensor_update.json")
        val entityId = "urn:ngsi-ld:UnknownType:0022CCC"

        every { entityService.exists(any()) } returns false

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity urn:ngsi-ld:UnknownType:0022CCC does not exist\"}")

        verify { entityService.exists(eq("urn:ngsi-ld:UnknownType:0022CCC")) }
    }

    @Test
    fun `delete entity should return a 204 if an entity has been successfully deleted`() {
        every { entityService.deleteEntity(any()) } returns Pair(1, 1)

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { entityService.deleteEntity(eq("urn:ngsi-ld:Sensor:0022CCC")) }
        confirmVerified(entityService)
    }

    @Test
    fun `delete entity should return a 404 if entity to be deleted has not been found`() {
        every { entityService.deleteEntity(any()) } returns Pair(0, 0)

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().isEmpty
    }

    @Test
    fun `delete entity should return a 500 if entity could not be deleted`() {
        every { entityService.deleteEntity(any()) } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                "\"title\":\"There has been an error during the operation execution\"," +
                "\"detail\":\"Unexpected server error\"}")
    }

    @Test
    fun `delete entity attribute should return a 204 if the attribute has been successfully deleted`() {
        every { entityService.exists(any()) } returns true
        every { entityService.deleteEntityAttribute(any(), any(), any()) } returns true

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:DeadFishes:019BN/attrs/fishNumber")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { entityService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { entityService.deleteEntityAttribute(eq("urn:ngsi-ld:DeadFishes:019BN"), eq("fishNumber"), eq(aquacContext!!)) }
        confirmVerified(entityService)
    }

    @Test
    fun `delete entity attribute should return a 404 if the entity is not found`() {
        every { entityService.exists(any()) } returns false

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:DeadFishes:019BN/attrs/fishNumber")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity Not Found\"}")
    }

    @Test
    fun `delete entity attribute should return a 404 if the attribute is not found`() {
        every { entityService.exists(any()) } returns true
        every { entityService.deleteEntityAttribute(any(), any(), any()) } throws ResourceNotFoundException("Attribute Not Found")
        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:DeadFishes:019BN/attrs/fishNumber")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Attribute Not Found\"}")

        verify { entityService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { entityService.deleteEntityAttribute(eq("urn:ngsi-ld:DeadFishes:019BN"), eq("fishNumber"), eq(aquacContext!!)) }
        confirmVerified(entityService)
    }

    @Test
    fun `delete entity attribute should return a 500 if the attribute could not be deleted`() {
        every { entityService.exists(any()) } returns true
        every { entityService.deleteEntityAttribute(any(), any(), any()) } returns false

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:DeadFishes:019BN/attrs/fishNumber")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"An error occurred while deleting fishNumber from urn:ngsi-ld:DeadFishes:019BN\"}")

        verify { entityService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { entityService.deleteEntityAttribute(eq("urn:ngsi-ld:DeadFishes:019BN"), eq("fishNumber"), eq(aquacContext!!)) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should support Mime-type with parameters`() {
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns mockkClass(ExpandedEntity::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .header(HttpHeaders.CONTENT_TYPE, "application/ld+json;charset=UTF-8")
            .exchange()
            .expectStatus().isOk
    }

    @Test
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .exchange()
            .expectStatus().isForbidden
    }
}
