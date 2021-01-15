package com.egm.stellio.entity.web

import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.model.AttributeType
import com.egm.stellio.entity.service.EntityTypeService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@WebFluxTest(EntityTypeHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", username = "mock-user")
class EntityTypeHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityTypeService: EntityTypeService

    private val apicHeaderLink = buildContextLinkHeader(APIC_COMPOUND_CONTEXT)

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
    @Test
    fun `get entity type information should return a 200 if entities of that type exists`() {
        every { entityTypeService.getEntityTypeInfo(any(), any()) } returns
            mockkClass(EntityTypeInfo::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/types/Beehive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        verify {
            entityTypeService.getEntityTypeInfo(
                "https://uri.etsi.org/ngsi-ld/default-context/Beehive",
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity type information should correctly serialize an EntityTypeInfo`() {
        every { entityTypeService.getEntityTypeInfo(any(), any()) } returns
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
            )

        webClient.get()
            .uri("/ngsi-ld/v1/types/BeeHive")
            .header(HttpHeaders.LINK, apicHeaderLink)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedEntityTypeInfo)

        verify {
            entityTypeService.getEntityTypeInfo(
                "https://ontology.eglobalmark.com/apic#BeeHive",
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity type information should search on entities with the expanded type if provided`() {
        every { entityTypeService.getEntityTypeInfo(any(), any()) } returns
            mockkClass(EntityTypeInfo::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/types/https%3A%2F%2Fontology.eglobalmark.com%2Fapic%23BeeHive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        verify {
            entityTypeService.getEntityTypeInfo(
                "https://ontology.eglobalmark.com/apic#BeeHive",
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get entity type information should return a 404 if no entities of that type exists`() {
        every { entityTypeService.getEntityTypeInfo(any(), any()) } returns null

        webClient.get()
            .uri("/ngsi-ld/v1/types/Beehive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
    }
}
