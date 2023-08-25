package com.egm.stellio.search.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.search.model.AttributeDetails
import com.egm.stellio.search.model.AttributeList
import com.egm.stellio.search.model.AttributeType
import com.egm.stellio.search.model.AttributeTypeInfo
import com.egm.stellio.search.service.AttributeService
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
@WebFluxTest(AttributeHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
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

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .build()
    }

    @Test
    fun `get attributes should return a 200 and an AttributeList`() {
        coEvery { attributeService.getAttributeList(any()) } returns AttributeList(
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

        coVerify {
            attributeService.getAttributeList(listOf(NGSILD_CORE_CONTEXT))
        }
    }

    @Test
    fun `get attributes should return a 200 and an empty attributeList if no attribute was found`() {
        coEvery { attributeService.getAttributeList(any()) } returns AttributeList(attributeList = emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/attributes")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isNotEmpty
            .jsonPath("$.type").isNotEmpty
            .jsonPath("$.attributeList").isEmpty

        coVerify {
            attributeService.getAttributeList(listOf(NGSILD_CORE_CONTEXT))
        }
    }

    @Test
    fun `get attribute should return a 200 and a list of Attributes if details param is true`() {
        coEvery { attributeService.getAttributeDetails(any()) } returns listOf(
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

        coVerify {
            attributeService.getAttributeDetails(listOf(NGSILD_CORE_CONTEXT))
        }
    }

    @Test
    fun `get attribute should return a 200 and an empty payload if no attribute was found and details param is true`() {
        coEvery { attributeService.getAttributeDetails(any()) } returns emptyList()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes?details=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        coVerify {
            attributeService.getAttributeDetails(listOf(NGSILD_CORE_CONTEXT))
        }
    }

    @Test
    fun `get attribute type information should return a 200 if attribute of that id exists`() {
        coEvery { attributeService.getAttributeTypeInfoByAttribute(any(), listOf(NGSILD_CORE_CONTEXT)) } returns
            mockkClass(AttributeTypeInfo::class, relaxed = true).right()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/temperature")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        coVerify {
            attributeService.getAttributeTypeInfoByAttribute(
                "https://uri.etsi.org/ngsi-ld/default-context/temperature",
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `get attribute type information should search on attributes with the expanded id if provided`() {
        coEvery { attributeService.getAttributeTypeInfoByAttribute(any(), listOf(NGSILD_CORE_CONTEXT)) } returns
            mockkClass(AttributeTypeInfo::class, relaxed = true).right()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/https%3A%2F%2Fontology.eglobalmark.com%2Fapic%23temperature")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        coVerify {
            attributeService.getAttributeTypeInfoByAttribute(TEMPERATURE_PROPERTY, listOf(NGSILD_CORE_CONTEXT))
        }
    }

    @Test
    fun `get attribute type information should correctly serialize an AttributeTypeInfo`() {
        coEvery { attributeService.getAttributeTypeInfoByAttribute(any(), listOf(APIC_COMPOUND_CONTEXT)) } returns
            AttributeTypeInfo(
                id = TEMPERATURE_PROPERTY.toUri(),
                type = "Attribute",
                attributeName = TEMPERATURE_COMPACT_PROPERTY,
                attributeTypes = setOf(AttributeType.Property),
                typeNames = setOf(BEEHIVE_COMPACT_TYPE),
                attributeCount = 2
            ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/temperature")
            .header(HttpHeaders.LINK, apicHeaderLink)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedAttributeTypeInfo)

        coVerify {
            attributeService.getAttributeTypeInfoByAttribute(TEMPERATURE_PROPERTY, listOf(APIC_COMPOUND_CONTEXT))
        }
    }

    @Test
    fun `get attribute type information should return a 404 if no attribute of that id exists`() {
        coEvery { attributeService.getAttributeTypeInfoByAttribute(any(), listOf(NGSILD_CORE_CONTEXT)) } returns
            ResourceNotFoundException(attributeNotFoundMessage(OUTGOING_PROPERTY)).left()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/$OUTGOING_COMPACT_PROPERTY")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"${attributeNotFoundMessage(OUTGOING_PROPERTY)}\"}"
            )
    }
}
