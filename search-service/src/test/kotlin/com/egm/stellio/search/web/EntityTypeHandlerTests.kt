package com.egm.stellio.search.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.AttributeType
import com.egm.stellio.search.service.EntityTypeService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockkClass
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@WebFluxTest(EntityTypeHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class EntityTypeHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityTypeService: EntityTypeService

    private val apicHeaderLink = buildContextLinkHeader(APIC_COMPOUND_CONTEXT)

    private val deadFishesType = "https://ontology.eglobalmark.com/aquac#DeadFishes"
    private val sensorType = "https://ontology.eglobalmark.com/egm#Sensor"

    private val expectedEntityTypeInfo =
        """
        {
           "id":"https://ontology.eglobalmark.com/apic#BeeHive",
           "type":"EntityTypeInfo",
           "typeName":"BeeHive",
           "entityCount":2,
           "attributeDetails":[
              {
                 "id":"https://ontology.eglobalmark.com/apic#temperature",
                 "type":"Attribute",
                 "attributeName":"temperature",
                 "attributeTypes":[
                    "Property"
                 ]
              },
              {
                 "id":"https://ontology.eglobalmark.com/egm#managedBy",
                 "type":"Attribute",
                 "attributeName":"managedBy",
                 "attributeTypes":[
                    "Relationship"
                 ]
              },
              {
                 "id":"https://uri.etsi.org/ngsi-ld/location",
                 "type":"Attribute",
                 "attributeName":"location",
                 "attributeTypes":[
                    "GeoProperty"
                 ]
              }
           ]
        }
        """.trimIndent()

    private val expectedEntityTypes =
        """
        [
           {
              "id":"https://ontology.eglobalmark.com/aquac#DeadFishes",
              "type":"EntityType",
              "typeName":"DeadFishes",
              "attributeNames":[
                 "https://ontology.eglobalmark.com/aquac#fishNumber",
                 "https://ontology.eglobalmark.com/aquac#removedFrom"
              ]
           },
           {
              "id":"https://ontology.eglobalmark.com/egm#Sensor",
              "type":"EntityType",
              "typeName":"Sensor",
              "attributeNames":[
                 "https://ontology.eglobalmark.com/aquac#isContainedIn",
                 "https://ontology.eglobalmark.com/aquac#deviceParameter"
              ]
           }
        ]
        """.trimIndent()

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .build()
    }

    @Test
    fun `get entity types should return a 200 and an EntityTypeList`() {
        coEvery { entityTypeService.getEntityTypeList(any()) } returns
            EntityTypeList(
                typeList = listOf(deadFishesType, sensorType)
            )

        webClient.get()
            .uri("/ngsi-ld/v1/types")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isNotEmpty
            .jsonPath("$.type").isNotEmpty
            .jsonPath("$.typeList").isNotEmpty

        coVerify {
            entityTypeService.getEntityTypeList(
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity types should return a 200 and an EntityTypeList with empty typeList if no entity was found`() {
        coEvery { entityTypeService.getEntityTypeList(any()) } returns EntityTypeList(typeList = emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/types")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isNotEmpty
            .jsonPath("$.type").isNotEmpty
            .jsonPath("$.typeList").isEmpty

        coVerify {
            entityTypeService.getEntityTypeList(
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity types should return a 200 and a list of EntityType if details param is true`() {
        coEvery { entityTypeService.getEntityTypes(any()) } returns listOf(
            EntityType(
                id = deadFishesType.toUri(),
                typeName = "DeadFishes",
                attributeNames = listOf(
                    "https://ontology.eglobalmark.com/aquac#fishNumber",
                    "https://ontology.eglobalmark.com/aquac#removedFrom"
                )
            ),
            EntityType(
                id = sensorType.toUri(),
                typeName = "Sensor",
                attributeNames = listOf(
                    "https://ontology.eglobalmark.com/aquac#isContainedIn",
                    "https://ontology.eglobalmark.com/aquac#deviceParameter"
                )
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/types?details=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedEntityTypes)

        coVerify {
            entityTypeService.getEntityTypes(
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity types should return a 200 and an empty payload if no entity was found and details param is true`() {
        coEvery { entityTypeService.getEntityTypes(any()) } returns emptyList()

        webClient.get()
            .uri("/ngsi-ld/v1/types?details=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        coVerify {
            entityTypeService.getEntityTypes(
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity type information should return a 200 if entities of that type exists`() {
        coEvery { entityTypeService.getEntityTypeInfoByType(any(), any()) } returns
            mockkClass(EntityTypeInfo::class, relaxed = true).right()

        webClient.get()
            .uri("/ngsi-ld/v1/types/Beehive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        coVerify {
            entityTypeService.getEntityTypeInfoByType(
                "https://uri.etsi.org/ngsi-ld/default-context/Beehive",
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity type information should correctly serialize an EntityTypeInfo`() {
        coEvery { entityTypeService.getEntityTypeInfoByType(any(), any()) } returns
            EntityTypeInfo(
                id = "https://ontology.eglobalmark.com/apic#BeeHive".toUri(),
                typeName = "BeeHive",
                entityCount = 2,
                attributeDetails = listOf(
                    AttributeInfo(
                        "https://ontology.eglobalmark.com/apic#temperature".toUri(),
                        "Attribute",
                        "temperature",
                        listOf(AttributeType.Property)
                    ),
                    AttributeInfo(
                        "https://ontology.eglobalmark.com/egm#managedBy".toUri(),
                        "Attribute",
                        "managedBy",
                        listOf(AttributeType.Relationship)
                    ),
                    AttributeInfo(
                        "https://uri.etsi.org/ngsi-ld/location".toUri(),
                        "Attribute",
                        "location",
                        listOf(AttributeType.GeoProperty)
                    )
                )
            ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/types/BeeHive")
            .header(HttpHeaders.LINK, apicHeaderLink)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedEntityTypeInfo)

        coVerify {
            entityTypeService.getEntityTypeInfoByType(
                "https://ontology.eglobalmark.com/apic#BeeHive",
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity type information should search on entities with the expanded type if provided`() {
        coEvery { entityTypeService.getEntityTypeInfoByType(any(), any()) } returns
            mockkClass(EntityTypeInfo::class, relaxed = true).right()

        webClient.get()
            .uri("/ngsi-ld/v1/types/https%3A%2F%2Fontology.eglobalmark.com%2Fapic%23BeeHive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        coVerify {
            entityTypeService.getEntityTypeInfoByType(
                "https://ontology.eglobalmark.com/apic#BeeHive",
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity type information should return a 404 if no entities of that type exists`() {
        coEvery { entityTypeService.getEntityTypeInfoByType(any(), any()) } returns
            ResourceNotFoundException(typeNotFoundMessage(BEEHIVE_TYPE)).left()

        webClient.get()
            .uri("/ngsi-ld/v1/types/$BEEHIVE_COMPACT_TYPE")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"${typeNotFoundMessage(BEEHIVE_TYPE)}\"}"
            )
    }
}
