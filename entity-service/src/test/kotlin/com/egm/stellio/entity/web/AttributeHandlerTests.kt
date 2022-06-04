package com.egm.stellio.entity.web

import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.AttributeDetails
import com.egm.stellio.entity.model.AttributeList
import com.egm.stellio.entity.model.AttributeTypeInfo
import com.egm.stellio.entity.service.AttributeService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.verify
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
@WithMockCustomUser(name = "Mock User", sub = "60AAEBA3-C0C7-42B6-8CB0-0D30857F210E")
class AttributeHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var attributeService: AttributeService

    private val apicHeaderLink = buildContextLinkHeader(APIC_COMPOUND_CONTEXT)

    private val expectedAttributeDetails =
        """
            [
               {
                  "id":"https://ontology.eglobalmark.com/apic#temperature",
                  "type":"Attribute",
                  "attributeName":"temperature",
                  "typeNames":[ "BeeHive" ]
               },
               {
                  "id":"https://ontology.eglobalmark.com/apic#incoming",
                  "type":"Attribute",
                  "attributeName":"incoming",
                  "typeNames":[ "BeeHive" ]
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
               "typeNames": ["BeeHive"],
               "attributeCount":2
            }
        """.trimIndent()

    @Test
    fun `get attributes should return a 200 and an AttributeList`() {
        every { attributeService.getAttributeList(any()) } returns AttributeList(
            attributeList = listOf(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY)
        )

        webClient.get()
            .uri("/ngsi-ld/v1/attributes")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isNotEmpty
            .jsonPath("$.type").isEqualTo("AttributeList")
            .jsonPath("$.attributeList").isEqualTo(
                listOfNotNull(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY)
            )

        verify {
            attributeService.getAttributeList(listOf(NGSILD_CORE_CONTEXT))
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
            attributeService.getAttributeList(listOf(NGSILD_CORE_CONTEXT))
        }
    }

    @Test
    fun `get attribute should return a 200 and a list of Attributes if details param is true`() {
        every { attributeService.getAttributeDetails(any()) } returns listOf(
            AttributeDetails(
                id = TEMPERATURE_PROPERTY.toUri(),
                attributeName = TEMPERATURE_COMPACT_PROPERTY,
                typeNames = setOf(BEEHIVE_COMPACT_TYPE)
            ),
            AttributeDetails(
                id = INCOMING_PROPERTY.toUri(),
                attributeName = INCOMING_COMPACT_PROPERTY,
                typeNames = setOf(BEEHIVE_COMPACT_TYPE)
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/attributes?details=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedAttributeDetails)

        verify {
            attributeService.getAttributeDetails(listOf(NGSILD_CORE_CONTEXT))
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
            attributeService.getAttributeDetails(listOf(NGSILD_CORE_CONTEXT))
        }
    }

    @Test
    fun `get attribute type information should return a 200 if attribute of that id exists`() {
        every { attributeService.getAttributeTypeInfo(any(), NGSILD_CORE_CONTEXT) } returns
            mockkClass(AttributeTypeInfo::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/temperature")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        verify {
            attributeService.getAttributeTypeInfo(
                "https://uri.etsi.org/ngsi-ld/default-context/temperature",
                NGSILD_CORE_CONTEXT
            )
        }
    }

    @Test
    fun `get attribute type information should search on attributes with the expanded id if provided`() {
        every { attributeService.getAttributeTypeInfo(any(), NGSILD_CORE_CONTEXT) } returns
            mockkClass(AttributeTypeInfo::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/https%3A%2F%2Fontology.eglobalmark.com%2Fapic%23temperature")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        verify {
            attributeService.getAttributeTypeInfo(TEMPERATURE_PROPERTY, NGSILD_CORE_CONTEXT)
        }
    }

    @Test
    fun `get attribute type information should correctly serialize an AttributeTypeInfo`() {
        every { attributeService.getAttributeTypeInfo(any(), APIC_COMPOUND_CONTEXT) } returns
            AttributeTypeInfo(
                id = TEMPERATURE_PROPERTY.toUri(),
                type = "Attribute",
                attributeName = TEMPERATURE_COMPACT_PROPERTY,
                attributeTypes = setOf("Property"),
                typeNames = setOf(BEEHIVE_COMPACT_TYPE),
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
            attributeService.getAttributeTypeInfo(TEMPERATURE_PROPERTY, APIC_COMPOUND_CONTEXT)
        }
    }

    @Test
    fun `get attribute type information should return a 404 if no attribute of that id exists`() {
        every { attributeService.getAttributeTypeInfo(any(), NGSILD_CORE_CONTEXT) } returns null

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/unknown")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
    }
}
