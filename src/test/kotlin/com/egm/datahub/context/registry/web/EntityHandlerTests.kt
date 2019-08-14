package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.GraphDBRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.hamcrest.core.Is
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.WebTestClient

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EntityHandlerTests(
) {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var graphDBRepository: GraphDBRepository

    @Test
    fun `should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/data/beehive.jsonld")
        every { graphDBRepository.createEntity(any(), any()) } returns ""
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC"))
    }

    @Test
    fun `should return a 400 if JSON-LD payload is not correct`() {
        every { graphDBRepository.createEntity(any(), any()) } returns ""
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody("{hophop}")
                .exchange()
                .expectStatus().isBadRequest
    }

    @Test
    fun `should return a 200 if entity exists`() {
        every { graphDBRepository.getById(any()) } returns ""
        webClient.get()
                .uri("/ngsi-ld/v1/entities/1234")
                .accept(MediaType.valueOf("application/ld+json"))
                .exchange()
                .expectStatus().isOk
    }
}