package com.egm.stellio.subscription.web

import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.removeNoise
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.config.ApplicationProperties
import com.egm.stellio.subscription.config.WebSecurityTestConfig
import com.egm.stellio.subscription.service.SubscriptionEventService
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.ParsingUtils.parseSubscriptionUpdate
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.hamcrest.core.Is
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpStatus
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@AutoConfigureWebTestClient(timeout = "30000")
@EnableConfigurationProperties(ApplicationProperties::class)
@ActiveProfiles("test")
@WebFluxTest(SubscriptionHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", username = "mock-user")
class SubscriptionHandlerTests {

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @MockkBean
    private lateinit var subscriptionEventService: SubscriptionEventService

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    private val subscriptionId = "urn:ngsi-ld:Subscription:1".toUri()

    @Test
    fun `get subscription by id should return 200 when subscription exists`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(true)
        every { subscriptionService.getById(any()) } returns Mono.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/${subscription.id}")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").doesNotExist()
            .jsonPath("$..modifiedAt").doesNotExist()

        verify { subscriptionService.exists(subscription.id) }
        verify { subscriptionService.isCreatorOf(subscription.id, "mock-user") }
        verify { subscriptionService.getById(subscription.id) }
    }

    @Test
    fun `get subscription by id should return 200 with sysAttrs when options query param specify it`() {
        val subscription = gimmeRawSubscription(withModifiedAt = true)

        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(true)
        every { subscriptionService.getById(any()) } returns Mono.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/${subscription.id}?options=sysAttrs")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").exists()
            .jsonPath("$..modifiedAt").exists()

        verify { subscriptionService.exists(subscription.id) }
        verify { subscriptionService.isCreatorOf(subscription.id, "mock-user") }
        verify { subscriptionService.getById(subscription.id) }
    }

    @Test
    fun `get subscription by id should return 404 when subscription does not exist`() {
        every { subscriptionService.exists(any()) } returns Mono.just(false)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\": \"${subscriptionNotFoundMessage(subscriptionId)}\"}"
            )

        verify { subscriptionService.exists(subscriptionId) }
    }

    @Test
    fun `get subscription by id should return a 403 if subscription does not belong to the user`() {
        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(false)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "detail":"${subscriptionUnauthorizedMessage(subscriptionId)}",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title":"The request tried to access an unauthorized resource"
                }
                """.trimIndent()
            )

        verify { subscriptionService.exists(subscriptionId) }
        verify { subscriptionService.isCreatorOf(subscriptionId, "mock-user") }
    }

    @Test
    fun `create subscription should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")
        val expectedOperationPayload = ClassPathResource("/ngsild/events/sent/subscription_create_event_payload.json")

        every { subscriptionService.exists(any()) } returns Mono.just(false)
        every { subscriptionService.create(any(), any()) } returns Mono.just(1)
        every { subscriptionEventService.publishSubscriptionEvent(any()) } returns true as java.lang.Boolean

        webClient.post()
            .uri("/ngsi-ld/v1/subscriptions")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1"))

        verify {
            subscriptionEventService.publishSubscriptionEvent(
                match {
                    it is EntityCreateEvent &&
                        it.operationType == EventsType.ENTITY_CREATE &&
                        it.entityId == "urn:ngsi-ld:Subscription:1".toUri() &&
                        it.operationPayload.removeNoise() == expectedOperationPayload.inputStream.readBytes()
                        .toString(Charsets.UTF_8).removeNoise() &&
                        it.contexts == listOf(apicContext!!)
                }
            )
        }
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
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/AlreadyExists\"," +
                    "\"title\":\"The referred element already exists\"," +
                    "\"detail\":\"${subscriptionAlreadyExistsMessage(subscriptionId)}\"}"
            )

        verify { subscriptionEventService wasNot called }
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
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"Internal Server Exception\"}"
            )
    }

    @Test
    fun `create subscription should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_incorrect_payload.json")

        @Suppress("MaxLineLength")
        webClient.post()
            .uri("/ngsi-ld/v1/subscriptions")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Request payload must contain @context term for a request having an application/ld+json content type"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `query subscriptions should return 200 without sysAttrs when options query param doesn't specify it`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(1)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions")
            .exchange()
            .expectStatus().isOk
            .expectHeader().doesNotExist("Link")
            .expectBody()
            .jsonPath("$[0].createdAt").doesNotExist()
            .jsonPath("$[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `query subscriptions should return 200 with sysAttrs when options query param specify it`() {
        val subscription = gimmeRawSubscription(withModifiedAt = true)

        every { subscriptionService.getSubscriptionsCount(any()) } returns Mono.just(1)
        every { subscriptionService.getSubscriptions(any(), any(), any()) } returns Flux.just(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions?options=sysAttrs")
            .exchange()
            .expectStatus().isOk
            .expectHeader().doesNotExist("Link")
            .expectBody()
            .jsonPath("$[0].createdAt").exists()
            .jsonPath("$[0].modifiedAt").exists()
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
            .expectHeader()
            .valueEquals("Link", "</ngsi-ld/v1/subscriptions?limit=1&page=1>;rel=\"prev\";type=\"application/ld+json\"")
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
            .expectHeader()
            .valueEquals("Link", "</ngsi-ld/v1/subscriptions?limit=1&page=2>;rel=\"next\";type=\"application/ld+json\"")
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
            .expectHeader().valueEquals(
                "Link",
                "</ngsi-ld/v1/subscriptions?limit=1&page=1>;rel=\"prev\";type=\"application/ld+json\"",
                "</ngsi-ld/v1/subscriptions?limit=1&page=3>;rel=\"next\";type=\"application/ld+json\""
            )
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
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Page number and Limit must be greater than zero"
                } 
                """.trimIndent()
            )
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
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Page number and Limit must be greater than zero"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `query subscriptions should return 400 if limit is greater than the maximum authorized limit`() {
        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=200&page=1")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Maximum limit is 100"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `update subscription should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val expectedOperationPayload = ClassPathResource("/ngsild/events/sent/subscription_update_event_payload.json")
        val subscriptionId = subscriptionId
        val parsedSubscription = parseSubscriptionUpdate(
            jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8),
            listOf(apicContext!!)
        )
        val updatedSubscription = gimmeRawSubscription()
        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(true)
        every { subscriptionService.update(any(), any()) } returns Mono.just(1)
        every { subscriptionService.getById(any()) } returns Mono.just(updatedSubscription)
        every { subscriptionEventService.publishSubscriptionEvent(any()) } returns true as java.lang.Boolean

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { subscriptionService.exists(eq((subscriptionId))) }
        verify { subscriptionService.isCreatorOf(subscriptionId, "mock-user") }
        verify { subscriptionService.update(eq(subscriptionId), parsedSubscription) }
        verify { subscriptionService.getById(eq(subscriptionId)) }
        verify {
            subscriptionEventService.publishSubscriptionEvent(
                match {
                    it is EntityUpdateEvent &&
                        it.operationType == EventsType.ENTITY_UPDATE &&
                        it.entityId == subscriptionId &&
                        it.operationPayload.removeNoise() == expectedOperationPayload.inputStream.readBytes()
                        .toString(Charsets.UTF_8).removeNoise() &&
                        it.updatedEntity == serializeObject(updatedSubscription)
                }
            )
        }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `update subscription should return a 500 if update in DB failed`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = subscriptionId
        val parsedSubscription = parseSubscriptionUpdate(
            jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8),
            listOf(apicContext!!)
        )

        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(true)
        every { subscriptionService.update(any(), any()) } throws RuntimeException("Update failed")

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().is5xxServerError
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"Update failed\"}"
            )

        verify { subscriptionService.exists(eq(subscriptionId)) }
        verify { subscriptionService.isCreatorOf(subscriptionId, "mock-user") }
        verify { subscriptionService.update(eq(subscriptionId), parsedSubscription) }
        verify { subscriptionEventService wasNot called }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `update subscription should return a 404 if subscription to be updated has not been found`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = subscriptionId

        every { subscriptionService.exists(any()) } returns Mono.just(false)

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"${subscriptionNotFoundMessage(subscriptionId)}\"}"
            )

        verify { subscriptionService.exists(eq(subscriptionId)) }
    }

    @Test
    fun `update subscription should return a 400 if JSON-LD context is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update_incorrect_payload.json")
        val subscriptionId = subscriptionId

        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(true)

        @Suppress("MaxLineLength")
        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Request payload must contain @context term for a request having an application/ld+json content type"
                } 
                """.trimIndent()
            )

        verify { subscriptionService.exists(eq(subscriptionId)) }
        verify { subscriptionService.isCreatorOf(subscriptionId, "mock-user") }
    }

    @Test
    fun `update subscription should return a 403 if subscription does not belong to the user`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = subscriptionId

        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(false)

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "detail":"${subscriptionUnauthorizedMessage(subscriptionId)}",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title":"The request tried to access an unauthorized resource"
                }
                """.trimIndent()
            )

        verify { subscriptionService.exists(eq(subscriptionId)) }
        verify { subscriptionService.isCreatorOf(subscriptionId, "mock-user") }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `delete subscription should return a 204 if a subscription has been successfully deleted`() {
        val subscription = gimmeRawSubscription()
        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(true)
        every { subscriptionService.delete(any()) } returns Mono.just(1)
        every { subscriptionEventService.publishSubscriptionEvent(any()) } returns true as java.lang.Boolean

        webClient.delete()
            .uri("/ngsi-ld/v1/subscriptions/${subscription.id}")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { subscriptionService.exists(subscription.id) }
        verify { subscriptionService.isCreatorOf(subscription.id, "mock-user") }
        verify { subscriptionService.delete(eq(subscription.id)) }
        verify {
            subscriptionEventService.publishSubscriptionEvent(
                match {
                    it is EntityDeleteEvent &&
                        it.operationType == EventsType.ENTITY_DELETE &&
                        it.entityId == subscription.id &&
                        it.contexts == listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
                }
            )
        }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `delete subscription should return a 404 if subscription to be deleted has not been found`() {
        every { subscriptionService.exists(any()) } returns Mono.just(false)

        webClient.delete()
            .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                        "detail":"${subscriptionNotFoundMessage(subscriptionId)}",
                        "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                        "title":"The referred resource has not been found"
                    }
                """.trimIndent()
            )

        verify { subscriptionService.exists(subscriptionId) }
        verify { subscriptionEventService wasNot called }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `delete subscription should return a 500 if subscription could not be deleted`() {
        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(true)
        every { subscriptionService.delete(any()) } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"Unexpected server error\"}"
            )

        verify { subscriptionService.exists(subscriptionId) }
        verify { subscriptionService.isCreatorOf(subscriptionId, "mock-user") }
        verify { subscriptionService.delete(eq(subscriptionId)) }
    }

    @Test
    fun `delete subscription should return a 403 if subscription does not belong to the user`() {
        every { subscriptionService.exists(any()) } returns Mono.just(true)
        every { subscriptionService.isCreatorOf(any(), any()) } returns Mono.just(false)

        webClient.delete()
            .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "detail":"${subscriptionUnauthorizedMessage(subscriptionId)}",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title":"The request tried to access an unauthorized resource"
                }
                """.trimIndent()
            )

        verify { subscriptionService.exists(subscriptionId) }
        verify { subscriptionService.isCreatorOf(subscriptionId, "mock-user") }
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
