package com.egm.stellio.search.discovery.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.discovery.model.AttributeDetails
import com.egm.stellio.search.discovery.model.AttributeList
import com.egm.stellio.search.discovery.model.AttributeType
import com.egm.stellio.search.discovery.model.AttributeTypeInfo
import com.egm.stellio.search.discovery.service.AttributeService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.config.FirewallConfig
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.ApiResponses.attributeNotFoundMessage
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.OUTGOING_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import com.egm.stellio.shared.util.UriUtils.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockkClass
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@WebFluxTest(AttributeHandler::class)
@Import(FirewallConfig::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class AttributeHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @Autowired
    private lateinit var applicationProperties: ApplicationProperties

    @MockkBean
    private lateinit var attributeService: AttributeService

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
            attributeList = listOf(INCOMING_TERM, OUTGOING_TERM)
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
                listOfNotNull(INCOMING_TERM, OUTGOING_TERM)
            )

        coVerify {
            attributeService.getAttributeList(listOf(applicationProperties.contexts.core))
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
            attributeService.getAttributeList(listOf(applicationProperties.contexts.core))
        }
    }

    @Test
    fun `get attribute should return a 200 and a list of Attributes if details param is true`() {
        coEvery { attributeService.getAttributeDetails(any()) } returns listOf(
            AttributeDetails(
                id = TEMPERATURE_IRI.toUri(),
                attributeName = TEMPERATURE_TERM,
                typeNames = setOf(BEEHIVE_TERM)
            ),
            AttributeDetails(
                id = INCOMING_IRI.toUri(),
                attributeName = INCOMING_TERM,
                typeNames = setOf(BEEHIVE_TERM)
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/attributes?details=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedAttributeDetails)

        coVerify {
            attributeService.getAttributeDetails(listOf(applicationProperties.contexts.core))
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
            attributeService.getAttributeDetails(listOf(applicationProperties.contexts.core))
        }
    }

    @Test
    fun `get attribute type information should return a 200 if attribute of that id exists`() {
        coEvery {
            attributeService.getAttributeTypeInfoByAttribute(any(), listOf(applicationProperties.contexts.core))
        } returns mockkClass(AttributeTypeInfo::class, relaxed = true).right()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/temperature")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        coVerify {
            attributeService.getAttributeTypeInfoByAttribute(
                "https://uri.etsi.org/ngsi-ld/default-context/temperature",
                listOf(applicationProperties.contexts.core)
            )
        }
    }

    @Test
    fun `get attribute type information should search on attributes with the expanded id if provided`() {
        coEvery {
            attributeService.getAttributeTypeInfoByAttribute(any(), listOf(applicationProperties.contexts.core))
        } returns mockkClass(AttributeTypeInfo::class, relaxed = true).right()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/https%3A%2F%2Fontology.eglobalmark.com%2Fapic%23temperature")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        coVerify {
            attributeService.getAttributeTypeInfoByAttribute(
                TEMPERATURE_IRI,
                listOf(applicationProperties.contexts.core)
            )
        }
    }

    @Test
    fun `get attribute type information should correctly serialize an AttributeTypeInfo`() {
        coEvery { attributeService.getAttributeTypeInfoByAttribute(any(), APIC_COMPOUND_CONTEXTS) } returns
            AttributeTypeInfo(
                id = TEMPERATURE_IRI.toUri(),
                type = "Attribute",
                attributeName = TEMPERATURE_TERM,
                attributeTypes = setOf(AttributeType.Property),
                typeNames = setOf(BEEHIVE_TERM),
                attributeCount = 2
            ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/temperature")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(expectedAttributeTypeInfo)

        coVerify {
            attributeService.getAttributeTypeInfoByAttribute(TEMPERATURE_IRI, APIC_COMPOUND_CONTEXTS)
        }
    }

    @Test
    fun `get attribute type information should return a 404 if no attribute of that id exists`() {
        coEvery {
            attributeService.getAttributeTypeInfoByAttribute(any(), listOf(applicationProperties.contexts.core))
        } returns ResourceNotFoundException(attributeNotFoundMessage(OUTGOING_IRI)).left()

        webClient.get()
            .uri("/ngsi-ld/v1/attributes/$OUTGOING_TERM")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                      "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                      "title":"${attributeNotFoundMessage(OUTGOING_IRI)}"
                    }
                """
            )
    }
}
