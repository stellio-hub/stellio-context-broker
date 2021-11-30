package com.egm.stellio.entity.web

import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.service.AttributeService
import com.egm.stellio.shared.WithMockCustomUser
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
@WebFluxTest(AttributeHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", username = "mock-user")
class AttributeHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var attributeService: AttributeService

    private val apicHeaderLink = buildContextLinkHeader(APIC_COMPOUND_CONTEXT)

    private val temperature = "https://ontology.eglobalmark.com/apic#temperature"
    private val deviceParameter = "https://ontology.eglobalmark.com/apic#deviceParameter"

    private val expectedAttributeDetails =
        """
            [
               {
                  "id":"https://ontology.eglobalmark.com/apic#temperature",
                  "type":"Attribute",
                  "attributeName":"temperature",
                  "typeNames":[
                     "https://ontology.eglobalmark.com/apic#Beehive"
                  ]
               },
               {
                  "id":"https://ontology.eglobalmark.com/apic#deviceParameter",
                  "type":"Attribute",
                  "attributeName":"deviceParameter",
                  "typeNames":[
                     "https://ontology.eglobalmark.com/apic#Beehive"
                  ]
               }
            ]
        """.trimIndent()

    private val expectedAttributeTypeInfo =
        """
            {
               "id":"https://ontology.eglobalmark.com/apic#temperature",
               "type":"Attribute",
               "attributeName": "temperature",
               "attributeTypes": ["Property"],
               "typeNames": ["Beehive"],
               "attributeCount":2
            }
        """.trimIndent()

    @Test
    fun `get attributes should return a 200 and an AttributeList`() {
        every { attributeService.getAttributeList(any()) } returns AttributeList(
            attributeList = listOf(
                "attributeService", "deviceParameter"
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/attributes")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isNotEmpty
            .jsonPath("$.type").isEqualTo("AttributeList")
            .jsonPath("$.attributeList").isEqualTo(listOfNotNull("attributeService", "deviceParameter"))

        verify {
            attributeService.getAttributeList(
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get attributes should return a 200 and an empty attributeList if no attribute was found`() {
        every { attributeService.getAttributeList(any()) } returns AttributeList(attributeList = emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/attributes")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isNotEmpty
            .jsonPath("$.type").isNotEmpty
            .jsonPath("$.attributeList").isEmpty

        verify {
            attributeService.getAttributeList(
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get attribute should return a 200 and a list of Attributes if details param is true`() {
        every { attributeService.getAttributeDetails(any()) } returns listOf(
            AttributeDetails(
                id = temperature.toUri(),
                attributeName = "temperature",
                typeNames = listOf(
                    "https://ontology.eglobalmark.com/apic#Beehive"
                )
            ),
            AttributeDetails(
                id = deviceParameter.toUri(),
                attributeName = "deviceParameter",
                typeNames = listOf(
                    "https://ontology.eglobalmark.com/apic#Beehive"
                )
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/attributes?details=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedAttributeDetails)

        verify {
            attributeService.getAttributeDetails(
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get attribute should return a 200 and an empty payload if no attribute was found and details param is true`() {
        every { attributeService.getAttributeDetails(any()) } returns emptyList()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes?details=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        verify {
            attributeService.getAttributeDetails(
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get attribute type information should return a 200 if attribute of that id exists`() {
        every { attributeService.getAttributeTypeInfo(any()) } returns
            mockkClass(AttributeTypeInfo::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/temperature")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        verify {
            attributeService.getAttributeTypeInfo(
                "https://uri.etsi.org/ngsi-ld/default-context/temperature"
            )
        }
    }

    @Test
    fun `get attribute type information should search on attributes with the expanded id if provided`() {
        every { attributeService.getAttributeTypeInfo(any()) } returns
            mockkClass(AttributeTypeInfo::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/https%3A%2F%2Fontology.eglobalmark.com%2Fapic%23temperature")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        verify {
            attributeService.getAttributeTypeInfo(
                "https://ontology.eglobalmark.com/apic#temperature"
            )
        }
    }

    @Test
    fun `get attribute type information should correctly serialize an AttributeTypeInfo`() {
        every { attributeService.getAttributeTypeInfo(any()) } returns
            AttributeTypeInfo(
                id = "https://ontology.eglobalmark.com/apic#temperature".toUri(),
                type = "Attribute",
                attributeName = "temperature",
                attributeTypes = listOf("Property"),
                typeNames = listOf("Beehive"),
                attributeCount = 2
            )

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/temperature")
            .header(HttpHeaders.LINK, apicHeaderLink)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedAttributeTypeInfo)

        verify {
            attributeService.getAttributeTypeInfo(
                "https://ontology.eglobalmark.com/apic#temperature"
            )
        }
    }

    @Test
    fun `get attribute type information should return a 404 if no attribute of that id exists`() {
        every { attributeService.getAttributeTypeInfo(any()) } returns null

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/unknown")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
    }
}
