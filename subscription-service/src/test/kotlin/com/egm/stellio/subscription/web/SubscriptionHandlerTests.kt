package com.egm.stellio.subscription.web

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.hamcrest.core.Is
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@OptIn(ExperimentalCoroutinesApi::class)
@AutoConfigureWebTestClient(timeout = "30000")
@ActiveProfiles("test")
@WebFluxTest(SubscriptionHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class)
class SubscriptionHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    private val subscriptionId = "urn:ngsi-ld:Subscription:1".toUri()

    @Test
    fun `get subscription by id should return 200 when subscription exists`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { subscriptionService.getById(any()) } returns subscription

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/${subscription.id}")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").doesNotExist()
            .jsonPath("$..modifiedAt").doesNotExist()

        coVerify { subscriptionService.exists(subscription.id) }
        coVerify { subscriptionService.isCreatorOf(subscription.id, sub) }
        coVerify { subscriptionService.getById(subscription.id) }
    }

    @Test
    fun `get subscription by id should return 200 with sysAttrs when options query param specify it`() = runTest {
        val subscription = gimmeRawSubscription(withModifiedAt = true)

        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { subscriptionService.getById(any()) } returns subscription

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/${subscription.id}?options=sysAttrs")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").exists()
            .jsonPath("$..modifiedAt").exists()

        coVerify { subscriptionService.exists(subscription.id) }
        coVerify { subscriptionService.isCreatorOf(subscription.id, sub) }
        coVerify { subscriptionService.getById(subscription.id) }
    }

    @Test
    fun `get subscription by id should return 404 when subscription does not exist`() = runTest {
        coEvery { subscriptionService.exists(any()) } returns false.right()

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\": \"${subscriptionNotFoundMessage(subscriptionId)}\"}"
            )

        coVerify { subscriptionService.exists(subscriptionId) }
    }

    @Test
    fun `get subscription by id should return a 403 if subscription does not belong to the user`() = runTest {
        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns false.right()

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

        coVerify { subscriptionService.exists(subscriptionId) }
        coVerify { subscriptionService.isCreatorOf(subscriptionId, sub) }
    }

    @Test
    fun `get subscription by id should return a 406 if the accept header is not correct`() {
        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
            .header("Accept", MediaType.APPLICATION_PDF.toString())
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE)
    }

    @Test
    fun `get subscription context should return a list of contexts`() = runTest {
        val subscription = gimmeRawSubscription().copy(contexts = listOf(APIC_COMPOUND_CONTEXT))
        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { subscriptionService.getContextsForSubscription(any()) } returns subscription.contexts.right()

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/${subscription.id}/context")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "@context" : [ "$APIC_COMPOUND_CONTEXT" ]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create subscription should return a 201 if JSON-LD payload is correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        coEvery { subscriptionService.validateNewSubscription(any()) } returns Unit.right()
        coEvery { subscriptionService.exists(any()) } returns false.right()
        coEvery { subscriptionService.create(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/subscriptions")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1"))
    }

    @Test
    fun `create subscription should return a 409 if the subscription already exists`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        coEvery { subscriptionService.validateNewSubscription(any()) } returns Unit.right()
        coEvery { subscriptionService.exists(any()) } returns true.right()

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
    }

    @Test
    fun `create subscription should return a 500 error if internal server Error`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        coEvery { subscriptionService.validateNewSubscription(any()) } returns Unit.right()
        coEvery { subscriptionService.exists(any()) } returns false.right()
        coEvery { subscriptionService.create(any(), any()) } throws InternalErrorException("Internal Server Exception")

        webClient.post()
            .uri("/ngsi-ld/v1/subscriptions")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(500)
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/InternalError",
                    "title":"There has been an error during the operation execution",
                    "detail":"InternalErrorException(message=Internal Server Exception)"
                }
                """
            )
    }

    @Test
    fun `create subscription should return a 400 if JSON-LD payload is not correct`() = runTest {
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
    fun `create subscription should return a 415 if the content type is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/subscription.json")

        webClient.post()
            .uri("/ngsi-ld/v1/subscriptions")
            .contentType(MediaType.APPLICATION_PDF)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE)

        coVerify { subscriptionService.validateNewSubscription(any()) wasNot Called }
    }

    @Test
    fun `create subscription should return a 400 if validation of the subscription fails`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_with_conflicting_timeInterval_watchedAttributes.json")

        coEvery { subscriptionService.exists(any()) } returns false.right()
        coEvery {
            subscriptionService.create(any(), any())
        } returns BadRequestDataException("You can't use 'timeInterval' with 'watchedAttributes' in conjunction").left()

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
                    "detail":"You can't use 'timeInterval' with 'watchedAttributes' in conjunction"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `query subscriptions should return 200 without sysAttrs when options query param doesn't specify it`() =
        runTest {
            val subscription = gimmeRawSubscription()

            coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(1)
            coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

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
    fun `query subscriptions should return 200 with sysAttrs when options query param specify it`() = runTest {
        val subscription = gimmeRawSubscription(withModifiedAt = true)

        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(1)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

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
    fun `query subscriptions should return 200 without link header`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(1)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/")
            .exchange()
            .expectStatus().isOk
            .expectHeader().doesNotExist("Link")
    }

    @Test
    fun `query subscriptions should return 200 with prev link header if exists`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(2)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&offset=2")
            .exchange()
            .expectStatus().isOk
            .expectHeader()
            .valueEquals(
                "Link",
                "</ngsi-ld/v1/subscriptions?limit=1&offset=1>;rel=\"prev\";type=\"application/ld+json\""
            )
    }

    @Test
    fun `query subscriptions should return 200 with next link header if exists`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(2)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&offset=0")
            .exchange()
            .expectStatus().isOk
            .expectHeader()
            .valueEquals(
                "Link",
                "</ngsi-ld/v1/subscriptions?limit=1&offset=1>;rel=\"next\";type=\"application/ld+json\""
            )
    }

    @Test
    fun `query subscriptions should return 200 with prev and next link header if exists`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(3)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&offset=1")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(
                "Link",
                "</ngsi-ld/v1/subscriptions?limit=1&offset=0>;rel=\"prev\";type=\"application/ld+json\"",
                "</ngsi-ld/v1/subscriptions?limit=1&offset=2>;rel=\"next\";type=\"application/ld+json\""
            )
    }

    @Test
    fun `query subscriptions should return 200 and empty response if requested offset does not exists`() = runTest {
        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(2)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns emptyList()

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&offset=9")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `query subscriptions should return 200 and the number of results if count is asked for`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(3)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions?${subscription.id}&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody()
    }

    @Test
    fun `query subscriptions should return 400 if requested offset is less than zero`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(2)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=1&offset=-1")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Offset must be greater than zero and limit must be strictly greater than zero"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `query subscriptions should return 400 if limit is equal or less than zero`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.getSubscriptionsCount(any()) } returns Either.Right(2)
        coEvery { subscriptionService.getSubscriptions(any(), any(), any()) } returns listOf(subscription)

        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=-1&offset=1")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Offset must be greater than zero and limit must be strictly greater than zero"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `query subscriptions should return 400 if limit is greater than the maximum authorized limit`() {
        webClient.get()
            .uri("/ngsi-ld/v1/subscriptions/?limit=200&offset=1")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"You asked for 200 results, but the supported maximum limit is 100"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `update subscription should return a 204 if JSON-LD payload is correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = subscriptionId
        val parsedSubscription = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8).deserializeAsMap()

        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { subscriptionService.update(any(), any(), any()) } returns Unit.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify { subscriptionService.exists(eq(subscriptionId)) }
        coVerify { subscriptionService.isCreatorOf(subscriptionId, sub) }
        coVerify { subscriptionService.update(eq(subscriptionId), parsedSubscription, listOf(APIC_COMPOUND_CONTEXT)) }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `update subscription should return a 500 if update in DB failed`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = subscriptionId
        val parsedSubscription = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8).deserializeAsMap()

        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { subscriptionService.update(any(), any(), any()) } throws RuntimeException("Update failed")

        webClient.patch()
            .uri("/ngsi-ld/v1/subscriptions/$subscriptionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().is5xxServerError
            .expectBody().json(
                """
                    {
                        "type":"https://uri.etsi.org/ngsi-ld/errors/InternalError",
                        "title":"There has been an error during the operation execution",
                        "detail":"java.lang.RuntimeException: Update failed"
                    }
                    """
            )

        coVerify { subscriptionService.exists(eq(subscriptionId)) }
        coVerify { subscriptionService.isCreatorOf(subscriptionId, sub) }
        coVerify { subscriptionService.update(eq(subscriptionId), parsedSubscription, listOf(APIC_COMPOUND_CONTEXT)) }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `update subscription should return a 404 if subscription to be updated has not been found`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = subscriptionId

        coEvery { subscriptionService.exists(any()) } returns false.right()

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

        coVerify { subscriptionService.exists(eq(subscriptionId)) }
    }

    @Test
    fun `update subscription should return a 400 if JSON-LD context is not correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update_incorrect_payload.json")
        val subscriptionId = subscriptionId

        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns true.right()

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

        coVerify { subscriptionService.exists(eq(subscriptionId)) }
        coVerify { subscriptionService.isCreatorOf(subscriptionId, sub) }
    }

    @Test
    fun `update subscription should return a 403 if subscription does not belong to the user`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/subscription_update.json")
        val subscriptionId = subscriptionId

        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns false.right()

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

        coVerify { subscriptionService.exists(eq(subscriptionId)) }
        coVerify { subscriptionService.isCreatorOf(subscriptionId, sub) }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `delete subscription should return a 204 if a subscription has been successfully deleted`() = runTest {
        val subscription = gimmeRawSubscription()
        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { subscriptionService.delete(any()) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/subscriptions/${subscription.id}")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify { subscriptionService.exists(subscription.id) }
        coVerify { subscriptionService.isCreatorOf(subscription.id, sub) }
        coVerify { subscriptionService.delete(eq(subscription.id)) }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `delete subscription should return a 404 if subscription to be deleted has not been found`() = runTest {
        coEvery { subscriptionService.exists(any()) } returns false.right()

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

        coVerify { subscriptionService.exists(subscriptionId) }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `delete subscription should return a 500 if subscription could not be deleted`() = runTest {
        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { subscriptionService.getContextsForSubscription(any()) } returns listOf(APIC_COMPOUND_CONTEXT).right()
        coEvery { subscriptionService.delete(any()) } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:1")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/InternalError",
                    "title":"There has been an error during the operation execution",
                    "detail":"java.lang.RuntimeException: Unexpected server error"
                }
                """
            )

        coVerify { subscriptionService.exists(subscriptionId) }
        coVerify { subscriptionService.isCreatorOf(subscriptionId, sub) }
        coVerify { subscriptionService.delete(eq(subscriptionId)) }
    }

    @Test
    fun `delete subscription should return a 403 if subscription does not belong to the user`() = runTest {
        coEvery { subscriptionService.exists(any()) } returns true.right()
        coEvery { subscriptionService.isCreatorOf(any(), any()) } returns false.right()

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

        coVerify { subscriptionService.exists(subscriptionId) }
        coVerify { subscriptionService.isCreatorOf(subscriptionId, sub) }
    }
}
