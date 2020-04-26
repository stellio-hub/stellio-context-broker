package com.egm.stellio.entity.web

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.service.Neo4jService
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_TIME_TYPE
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.hamcrest.core.Is
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@WithMockUser
class EntityHandlerTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var neo4jService: Neo4jService

    @Test
    fun `create entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214"

        every { neo4jService.exists(any()) } returns false
        every { neo4jService.createEntity(any(), any()) } returns Entity(id = breedingServiceId, type = listOf("BeeHive"))

        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/$breedingServiceId"))
    }

    @Test
    fun `create entity should return a 409 if the entity already exists`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        every { neo4jService.exists(any()) } returns true

        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(409)
    }

    @Test
    fun `create entity should return a 500 error if internal server Error`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        every { neo4jService.exists(any()) } returns false
        every { neo4jService.createEntity(any(), any()) } throws InternalErrorException("Internal Server Exception")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(500)
    }

    @Test
    fun `create entity should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
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
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `get entity by id should return 200 when entity exists`() {

        every { neo4jService.exists(any()) } returns true
        every { neo4jService.getFullEntityById(any()) } returns Pair(emptyMap(), emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `get entity by id should correctly serialize temporal properties`() {

        every { neo4jService.exists(any()) } returns true
        every { neo4jService.getFullEntityById(any()) } returns Pair(
            mapOf(NGSILD_CREATED_AT_PROPERTY to mapOf("@type" to NGSILD_DATE_TIME_TYPE,
                "@value" to OffsetDateTime.of(LocalDateTime.of(2015, 10, 18, 11, 20, 30, 1000), ZoneOffset.of("+1")))),
            listOf(NGSILD_CORE_CONTEXT)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\"createdAt\":\"2015-10-18T11:20:30.000001+01:00\",\"@context\":\"$NGSILD_CORE_CONTEXT\"}")
    }

    @Test
    fun `get entity by id should correctly serialize properties of type DateTime`() {

        every { neo4jService.exists(any()) } returns true
        every { neo4jService.getFullEntityById(any()) } returns Pair(
            mapOf("https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf("@type" to "https://uri.etsi.org/ngsi-ld/Property",
                NGSILD_PROPERTY_VALUE to mapOf(
                    "@type" to NGSILD_DATE_TIME_TYPE,
                    "@value" to ZonedDateTime.of(LocalDateTime.of(2015, 10, 18, 11, 20, 30, 1000), ZoneOffset.of("+1"))))),
            listOf(NGSILD_CORE_CONTEXT)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\"testedAt\":{\"type\":\"Property\",\"value\":{\"type\":\"DateTime\",\"@value\":\"2015-10-18T11:20:30.000001+01:00\"}},\"@context\":\"$NGSILD_CORE_CONTEXT\"}")
    }

    @Test
    fun `get entity by id should correctly serialize properties of type Date`() {

        every { neo4jService.exists(any()) } returns true
        every { neo4jService.getFullEntityById(any()) } returns Pair(
            mapOf("https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf("@type" to "https://uri.etsi.org/ngsi-ld/Property",
                NGSILD_PROPERTY_VALUE to mapOf(
                    "@type" to NGSILD_DATE_TYPE,
                    "@value" to LocalDate.of(2015, 10, 18)))),
            listOf(NGSILD_CORE_CONTEXT)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\"testedAt\":{\"type\":\"Property\",\"value\":{\"type\":\"Date\",\"@value\":\"2015-10-18\"}},\"@context\":\"$NGSILD_CORE_CONTEXT\"}")
    }

    @Test
    fun `get entity by id should correctly serialize properties of type Time`() {

        every { neo4jService.exists(any()) } returns true
        every { neo4jService.getFullEntityById(any()) } returns Pair(
            mapOf("https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf("@type" to "https://uri.etsi.org/ngsi-ld/Property",
                NGSILD_PROPERTY_VALUE to mapOf(
                    "@type" to NGSILD_TIME_TYPE,
                    "@value" to LocalTime.of(11, 20, 30)))),
            listOf(NGSILD_CORE_CONTEXT)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\"testedAt\":{\"type\":\"Property\",\"value\":{\"type\":\"Time\",\"@value\":\"11:20:30\"}},\"@context\":\"$NGSILD_CORE_CONTEXT\"}")
    }

    @Test
    fun `get entity by id should return 404 when entity does not exist`() {

        every { neo4jService.exists(any()) } returns false

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    fun `search on entities should return 400 if required parameters are missing`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `append entity attribute should return a 204 if all attributes were appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { neo4jService.exists(any()) } returns true
        every { neo4jService.appendEntityAttributes(any(), any(), any()) } returns UpdateResult(listOf("fishNumber"), emptyList())

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { neo4jService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }
        verify { neo4jService.appendEntityAttributes(eq("urn:ngsi-ld:BreedingService:0214"), any(), eq(false)) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 207 if some attributes could not be appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { neo4jService.exists(any()) } returns true
        every { neo4jService.appendEntityAttributes(any(), any(), any()) }
            .returns(UpdateResult(listOf("fishNumber"), listOf(NotUpdatedDetails("wrongAttribute", "overwrite disallowed"))))

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json("{\"updated\":[\"fishNumber\"],\"notUpdated\":[{\"attributeName\":\"wrongAttribute\",\"reason\":\"overwrite disallowed\"}]}")

        verify { neo4jService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }
        verify { neo4jService.appendEntityAttributes(eq("urn:ngsi-ld:BreedingService:0214"), any(), eq(false)) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { neo4jService.exists(any()) } returns false

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound

        verify { neo4jService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 400 if the attribute is missing a type`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty_missing_type.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { neo4jService.exists(any()) } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest

        verify { neo4jService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 400 if the attribute is a property and is missing a value`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty_missing_value.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { neo4jService.exists(any()) } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest

        verify { neo4jService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 400 if the attribute is a relationship and is missing an object`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newRelationship_missing_object.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214"

        every { neo4jService.exists(any()) } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest

        verify { neo4jService.exists(eq("urn:ngsi-ld:BreedingService:0214")) }

        confirmVerified()
    }

    @Test
    fun `partial attribute update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN"
        val attrId = "fishNumber"

        every { neo4jService.updateEntityAttribute(any(), any(), any(), any()) } returns 1

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { neo4jService.updateEntityAttribute(eq(entityId), eq(attrId), any(), eq(aquacContext!!)) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `entity attributes update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN"

        every { neo4jService.exists(any()) } returns true
        every { neo4jService.updateEntityAttributes(any(), any(), any()) } returns UpdateResult(updated = arrayListOf("fishNumber"), notUpdated = arrayListOf())

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { neo4jService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { neo4jService.updateEntityAttributes(eq(entityId), any(), eq(aquacContext!!)) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `entity attributes update should return a 207 if some attributes are not found`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate_attributeNotFound.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN"
        val notUpdatedAttribute = NotUpdatedDetails("unknownAttribute", "Property Not Found")
        every { neo4jService.exists(any()) } returns true
        every { neo4jService.updateEntityAttributes(any(), any(), any()) } returns UpdateResult(updated = arrayListOf("fishNumber"), notUpdated = arrayListOf(notUpdatedAttribute))

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(207)

        verify { neo4jService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { neo4jService.updateEntityAttributes(eq(entityId), any(), eq(aquacContext!!)) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `entity attributes update should return a 207 if some relationships objects are not found`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate_relationshipObjectNotFound.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN"
        val notUpdatedAttribute = NotUpdatedDetails("removedFrom",
            "Target entity unknownObject in property does not exist, create it first")
        every { neo4jService.exists(any()) } returns true
        every { neo4jService.updateEntityAttributes(any(), any(), any()) } returns UpdateResult(updated = arrayListOf("fishNumber"), notUpdated = arrayListOf(notUpdatedAttribute))

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)

        verify { neo4jService.exists(eq("urn:ngsi-ld:DeadFishes:019BN")) }
        verify { neo4jService.updateEntityAttributes(eq(entityId), any(), eq(aquacContext!!)) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `entity attributes update should return a 400 if JSON-LD context is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/sensor_update.json")
        val entityId = "urn:ngsi-ld:Sensor:0022CCC"

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `entity attributes update should return a 404 if entity type is unknown`() {
        val jsonLdFile = ClassPathResource("/ngsild/sensor_update.json")
        val entityId = "urn:ngsi-ld:UnknownType:0022CCC"

        every { neo4jService.exists(any()) } returns false

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound

        verify { neo4jService.exists(eq("urn:ngsi-ld:UnknownType:0022CCC")) }
    }

    @Test
    fun `delete entity should return a 204 if an entity has been successfully deleted`() {
        every { neo4jService.deleteEntity(any()) } returns Pair(1, 1)

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .accept(MediaType.valueOf("application/ld+json"))
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { neo4jService.deleteEntity(eq("urn:ngsi-ld:Sensor:0022CCC")) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `delete entity should return a 404 if entity to be deleted has not been found`() {
        every { neo4jService.deleteEntity(any()) } returns Pair(0, 0)

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .accept(MediaType.valueOf("application/ld+json"))
            .exchange()
            .expectStatus().isNotFound
            .expectBody().isEmpty
    }

    @Test
    fun `delete entity should return a 500 if entity could not be deleted`() {
        every { neo4jService.deleteEntity(any()) } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .accept(MediaType.valueOf("application/ld+json"))
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json("{\"ProblemDetails\":[\"Unexpected server error\"]}")
    }

    @Test
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .exchange()
            .expectStatus().isForbidden
    }
}
