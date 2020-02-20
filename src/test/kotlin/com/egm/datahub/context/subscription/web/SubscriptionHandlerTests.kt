package com.egm.datahub.context.subscription.web

import com.egm.datahub.context.subscription.service.SubscriptionService
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.hamcrest.core.Is
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.security.test.context.support.WithMockUser
import reactor.core.publisher.Mono

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@WithMockUser
class SubscriptionHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @Test
    fun `create subscription should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        every { subscriptionService.exists(any()) } returns Mono.just(false)
        every { subscriptionService.create(any()) } returns Mono.empty()

        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
                .header("Link:rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:04"))
    }

    @Test
    fun `create subscription should return a 409 if the subscription already exists`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        every { subscriptionService.exists(any()) } returns Mono.just(true)

        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
                .header("Link:rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(409)
    }

    @Test
    fun `create subscription should return a 500 error if internal server Error`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        every { subscriptionService.exists(any()) } returns Mono.just(false)
        every { subscriptionService.create(any()) } throws InternalErrorException("Internal Server Exception")

        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
                .header("Link:rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(500)
    }

    @Test
    fun `create subscription should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_incorrect_payload.json")

        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
                .header("Link:rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isBadRequest
    }

    @Test
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
                .header("Link:rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .exchange()
                .expectStatus().isForbidden
    }
}
