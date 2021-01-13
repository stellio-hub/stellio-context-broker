package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.service.EntityTypeService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
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

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @Test
    fun `get entity type information should return a 200 if entities of that type exists`() {
        val authorizedEntitiesIds = listOf("urn:ngsi-ld:Beehive:TESTC".toUri())

        every { entityTypeService.getEntitiesByType(any()) } returns listOf(
            JsonLdEntity(
                mapOf(
                    "@id" to "urn:ngsi-ld:Beehive:TESTC",
                    "@type" to listOf("Beehive")
                ),
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        every { authorizationService.filterEntitiesUserCanRead(any(), any()) } returns authorizedEntitiesIds
        every { entityTypeService.getEntityTypeInformation(any(), any()) } returns
            mockkClass(EntityTypeInfo::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/types/Beehive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        verify { entityTypeService.getEntitiesByType("https://uri.etsi.org/ngsi-ld/default-context/Beehive") }
    }

    @Test
    fun `get entity type information should search on entities with the expanded type if provided`() {
        val authorizedEntitiesIds = listOf("urn:ngsi-ld:Beehive:TESTC".toUri())

        every { entityTypeService.getEntitiesByType(any()) } returns listOf(
            JsonLdEntity(
                mapOf(
                    "@id" to "urn:ngsi-ld:Beehive:TESTC",
                    "@type" to listOf("Beehive")
                ),
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        every { authorizationService.filterEntitiesUserCanRead(any(), any()) } returns authorizedEntitiesIds
        every { entityTypeService.getEntityTypeInformation(any(), any()) } returns
            mockkClass(EntityTypeInfo::class, relaxed = true)

        webClient.get()
            .uri("/ngsi-ld/v1/types/https%3A%2F%2Fontology.eglobalmark.com%2Fapic%23BeeHive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk

        verify { entityTypeService.getEntitiesByType("https://ontology.eglobalmark.com/apic#BeeHive") }
    }

    @Test
    fun `get entity type information should return a 404 if no entities of that type exists`() {
        every { entityTypeService.getEntitiesByType(any()) } returns emptyList()

        webClient.get()
            .uri("/ngsi-ld/v1/types/Beehive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    fun `get entity type information should return a 403 if user is not authorized to read entities`() {
        every { entityTypeService.getEntitiesByType(any()) } returns listOf(
            JsonLdEntity(
                mapOf(
                    "@id" to "urn:ngsi-ld:Beehive:TESTC",
                    "@type" to listOf("Beehive")
                ),
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        every { authorizationService.filterEntitiesUserCanRead(any(), any()) } returns emptyList()

        webClient.get()
            .uri("/ngsi-ld/v1/types/Beehive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isForbidden
    }
}
