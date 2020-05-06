package com.egm.stellio.subscription.web

import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.subscription.config.WithMockCustomUser
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscriptionUpdate
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.hamcrest.core.Is
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.security.test.context.support.WithAnonymousUser
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.lang.RuntimeException

@AutoConfigureWebTestClient(timeout = "30000")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@WithMockCustomUser(name = "Mock User", username = "mock-user")
class SubscriptionHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @Test
    fun `get subscription by id should return 200 when subscription exists`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.getById(any(), any()) } returns Mono.just(subscription)

        webClient.get()
                .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:${subscription.id}")
                .exchange()
                .expectStatus().isOk
    }

    @Test
    fun `get subscription by id should return 404 when subscription does not exist`() {
        every { subscriptionService.getById(any(), any()) } returns Mono.error(ResourceNotFoundException("Subscription Not Found"))

        webClient.get()
                .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
                .exchange()
                .expectStatus().isNotFound
                .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Subscription Not Found\"}")
    }

    @Test
    fun `create subscription should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        every { subscriptionService.exists(any()) } returns Mono.just(false)
        every { subscriptionService.create(any(), any()) } returns Mono.just(1)

        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
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
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/AlreadyExists\"," +
                    "\"title\":\"The referred element already exists\"," +
                    "\"detail\":\"A subscription with id urn:ngsi-ld:Subscription:04 already exists\"}")
    }

    @Test
    fun `create subscription should return a 500 error if internal server Error`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        every { subscriptionService.exists(any()) } returns Mono.just(false)
        every { subscriptionService.create(any(), any()) } throws InternalErrorException("Internal Server Exception")

        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(500)
    }

    @Test
    fun `create subscription should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_incorrect_payload.json")

        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isBadRequest
    }

    @Test
    fun `query subscriptions should return 200 without link header`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(1)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/")
            .exchange()
            .expectStatus().isOk
            .expectHeader().doesNotExist("Link")
    }

    @Test
    fun `query subscriptions should return 200 with prev link header if exists`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(2)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&page=2")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals("Link", "</ngsi-ld/v1/subscriptions?limit=1&page=1>;rel=\"prev\";type=\"application/ld+json\"")
    }

    @Test
    fun `query subscriptions should return 200 with next link header if exists`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(2)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&page=1")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals("Link", "</ngsi-ld/v1/subscriptions?limit=1&page=2>;rel=\"next\";type=\"application/ld+json\"")
    }

    @Test
    fun `query subscriptions should return 200 with prev and next link header if exists`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(3)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&page=2")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals("Link",
        "</ngsi-ld/v1/subscriptions?limit=1&page=1>;rel=\"prev\";type=\"application/ld+json\"", "</ngsi-ld/v1/subscriptions?limit=1&page=3>;rel=\"next\";type=\"application/ld+json\"")
    }

    @Test
    fun `query subscriptions should return 200 and empty response if requested page does not exists`() {

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(2)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.empty()

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&page=9")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `query subscriptions should return 400 if requested page is equal or less than zero`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(2)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&page=0")
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `query subscriptions should return 400 if limit is equal or less than zero`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(2)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=-1&page=1")
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `update subscription should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = "urn:ngsi-ld:Subscription:04"
        val parsedSubscription = parseSubscriptionUpdate(jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8))

        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.update(any(), any(), any()) } returns Mono.just(1)

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { subscriptionService.exists(eq("urn:ngsi-ld:Subscription:04")) }
        verify { subscriptionService.update(eq(subscriptionId), parsedSubscription, "mock-user") }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `update subscription should return a 500 if update in DB failed`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = "urn:ngsi-ld:Subscription:04"
        val parsedSubscription = parseSubscriptionUpdate(jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8))

        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.update(any(), any(), any()) } throws RuntimeException("Update failed")

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().is5xxServerError
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"Update failed\"}")

        verify { subscriptionService.exists(eq("urn:ngsi-ld:Subscription:04")) }
        verify { subscriptionService.update(eq(subscriptionId), parsedSubscription, "mock-user") }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `update subscription should return a 404 if subscription to be updated has not been found`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = "urn:ngsi-ld:Subscription:04"

        every { subscriptionService.exists(any()) } returns Mono.just(false)

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Could not find a subscription with id urn:ngsi-ld:Subscription:04\"}")

        verify { subscriptionService.exists(eq("urn:ngsi-ld:Subscription:04")) }
    }

    @Test
    fun `update subscription should return a 400 if JSON-LD context is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update_incorrect_payload.json")
        val subscriptionId = "urn:ngsi-ld:Subscription:04"

        every { subscriptionService.exists(any()) } returns Mono.just(true)
        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"Context not provided\"}")
    }

    @Test
    fun `delete subscription should return a 204 if a subscription has been successfully deleted`() {
        val subscription = gimmeRawSubscription()
        every { subscriptionService.delete(any(), any()) } returns Mono.just(1)

        webClient.delete()
                .uri("/ngsi-ld/v1/subscriptions/${subscription.id}")
                .exchange()
                .expectStatus().isNoContent
                .expectBody().isEmpty

        verify { subscriptionService.delete(eq(subscription.id), "mock-user") }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `delete subscription should return a 404 if subscription to be deleted has not been found`() {
        every { subscriptionService.delete(any(), any()) } returns Mono.just(0)

        webClient.delete()
                .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
                .exchange()
                .expectStatus().isNotFound
                .expectBody().isEmpty
    }

    @Test
    fun `delete subscription should return a 500 if subscription could not be deleted`() {
        every { subscriptionService.delete(any(), any()) } throws RuntimeException("Unexpected server error")

        webClient.delete()
                .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"Unexpected server error\"}")
    }

    @Test
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.post()
                .uri("/ngsi-ld/v1/subscriptions")
                .exchange()
                .expectStatus().isForbidden
    }
}