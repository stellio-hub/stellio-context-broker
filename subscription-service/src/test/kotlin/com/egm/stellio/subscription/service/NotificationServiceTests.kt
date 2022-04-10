package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.EGM_BASE_CONTEXT_URL
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.config.ApplicationProperties
import com.egm.stellio.subscription.firebase.FCMService
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [NotificationService::class])
@ActiveProfiles("test")
class NotificationServiceTests {

    @MockkBean
    private lateinit var applicationProperties: ApplicationProperties

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @MockkBean
    private lateinit var subscriptionEventService: SubscriptionEventService

    @MockkBean
    private lateinit var fcmService: FCMService

    @Autowired
    private lateinit var notificationService: NotificationService

    private lateinit var wireMockServer: WireMockServer

    private val apiaryId = "urn:ngsi-ld:Apiary:XYZ01"

    private val contexts = listOf(
        "$EGM_BASE_CONTEXT_URL/apic/jsonld-contexts/apic.jsonld",
        "$EGM_BASE_CONTEXT_URL/shared-jsonld-contexts/egm.jsonld",
        NGSILD_CORE_CONTEXT
    )

    private val rawEntity =
        """
            {
               "id":"$apiaryId",
               "type":"Apiary",
               "name":{
                  "type":"Property",
                  "value":"ApiarySophia"
               },
               "excludedProp":{
                  "type":"Property",
                  "value":"excluded"
               },
               "location": {
                  "type": "GeoProperty",
                  "value": {
                     "type": "Point",
                     "coordinates": [
                        24.30623,
                        60.07966
                     ]
                  }
               },
               "@context":[
                  "${contexts.joinToString("\",\"")}"                  
               ]
            } 
        """.trimIndent()

    @BeforeAll
    fun beforeAll() {
        wireMockServer = WireMockServer(wireMockConfig().port(8089))
        wireMockServer.start()
        // If not using the default port, we need to instruct explicitly the client (quite redundant)
        configureFor(8089)
    }

    @AfterEach
    fun resetWiremock() {
        reset()
    }

    @AfterAll
    fun afterAll() {
        wireMockServer.stop()
    }

    @Test
    fun `it should notify the subscriber and update the subscription`() {
        val subscription = gimmeRawSubscription()
        val parsedEntity = expandJsonLdEntity(rawEntity, subscription.contexts).toNgsiLdEntity()

        every { subscriptionService.getMatchingSubscriptions(any(), any(), any()) } answers { Flux.just(subscription) }
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(any(), any()) } answers { Mono.just(true) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        val notificationResult =
            notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity, setOf("name"))

        StepVerifier.create(notificationResult)
            .expectNextMatches {
                it.size == 1 &&
                    it[0].second.subscriptionId == subscription.id &&
                    it[0].second.data.size == 1 &&
                    it[0].third
            }
            .expectComplete()
            .verify()

        verify(timeout = 1000, exactly = 1) {
            subscriptionEventService.publishNotificationCreateEvent(
                null,
                match { it.subscriptionId == subscription.id }
            )
        }

        verify {
            subscriptionService.getMatchingSubscriptions(
                apiaryId.toUri(),
                listOf("https://uri.etsi.org/ngsi-ld/default-context/Apiary"),
                "name"
            )
        }
        verify { subscriptionService.isMatchingQuery(subscription.q, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription.id, any()) }
        verify { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should notify the subscriber and only keep the expected attributes`() {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(
                FormatType.NORMALIZED,
                listOf("https://uri.etsi.org/ngsi-ld/default-context/name", "https://uri.etsi.org/ngsi-ld/location")
            )
        )
        val parsedEntity = expandJsonLdEntity(rawEntity, subscription.contexts).toNgsiLdEntity()

        every { subscriptionService.getMatchingSubscriptions(any(), any(), any()) } answers { Flux.just(subscription) }
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(any(), any()) } answers { Mono.just(true) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        val notificationResult =
            notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity, setOf("name"))

        StepVerifier.create(notificationResult)
            .expectNextMatches {
                it.size == 1 &&
                    it[0].second.subscriptionId == subscription.id &&
                    it[0].second.data.size == 1 &&
                    it[0].second.data[0].size == 5 &&
                    it[0].second.data[0].all { entry ->
                        listOf("id", "type", "name", "location", "@context").contains(entry.key)
                    } &&
                    it[0].third
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should send a simplified payload when format is keyValues and include only the specified attributes`() {
        val subscription = gimmeRawSubscription(withNotifParams = Pair(FormatType.KEY_VALUES, listOf("location")))
        val parsedEntity = expandJsonLdEntity(rawEntity, subscription.contexts).toNgsiLdEntity()

        every { subscriptionService.getMatchingSubscriptions(any(), any(), any()) } answers { Flux.just(subscription) }
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(any(), any()) } answers { Mono.just(true) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        val notificationResult =
            notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity, setOf("name"))

        StepVerifier.create(notificationResult)
            .expectNextMatches {
                it.size == 1 &&
                    it[0].second.subscriptionId == subscription.id &&
                    it[0].second.data.size == 1 &&
                    it[0].third
            }
            .expectComplete()
            .verify()

        verify(timeout = 1000, exactly = 1) {
            subscriptionEventService.publishNotificationCreateEvent(
                null,
                match { it.subscriptionId == subscription.id }
            )
        }

        verify {
            subscriptionService.getMatchingSubscriptions(
                apiaryId.toUri(),
                listOf("https://uri.etsi.org/ngsi-ld/default-context/Apiary"),
                "name"
            )
        }
        verify { subscriptionService.isMatchingQuery(subscription.q, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription.id, any()) }
        verify { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should notify the two subscribers`() {
        val subscription1 = gimmeRawSubscription()
        val subscription2 = gimmeRawSubscription()
        val parsedEntity = expandJsonLdEntity(rawEntity, subscription1.contexts).toNgsiLdEntity()

        every { subscriptionService.getMatchingSubscriptions(any(), any(), any()) } answers {
            Flux.just(subscription1, subscription2)
        }
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(any(), any()) } answers { Mono.just(true) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        val notificationResult =
            notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity, setOf("name"))

        StepVerifier.create(notificationResult)
            .expectNextMatches {
                it.size == 2
            }
            .expectComplete()
            .verify()

        verify {
            subscriptionService.getMatchingSubscriptions(
                apiaryId.toUri(),
                listOf("https://uri.etsi.org/ngsi-ld/default-context/Apiary"),
                "name"
            )
        }
        verify { subscriptionService.isMatchingQuery(subscription1.q, any()) }
        verify { subscriptionService.isMatchingQuery(subscription2.q, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription1.id, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription2.id, any()) }
        verify(exactly = 2) { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should notify the second subscriber even if the first notification has failed`() {
        // the notification for this one is expected to fail as we don't listen on port 8088
        val subscription1 = gimmeRawSubscription()
            .copy(
                notification = NotificationParams(
                    attributes = emptyList(),
                    format = FormatType.KEY_VALUES,
                    endpoint = Endpoint(
                        uri = "http://localhost:8088/notification".toUri(),
                        accept = Endpoint.AcceptType.JSONLD,
                        info = null
                    ),
                    status = null,
                    lastNotification = null,
                    lastFailure = null,
                    lastSuccess = null
                )
            )
        val subscription2 = gimmeRawSubscription()
        val parsedEntity = expandJsonLdEntity(rawEntity, subscription1.contexts).toNgsiLdEntity()

        every { subscriptionService.getMatchingSubscriptions(any(), any(), any()) } answers {
            Flux.just(subscription1, subscription2)
        }
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(any(), any()) } answers { Mono.just(true) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        val notificationResult = notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity, setOf("name"))

        StepVerifier.create(notificationResult)
            .expectNextMatches { results ->
                results.size == 2 &&
                    results.all {
                        it.first.id == subscription1.id && !it.third ||
                            it.first.id == subscription2.id && it.third
                    }
            }
            .expectComplete()
            .verify()

        verify {
            subscriptionService.getMatchingSubscriptions(
                apiaryId.toUri(),
                listOf("https://uri.etsi.org/ngsi-ld/default-context/Apiary"),
                "name"
            )
        }
        verify { subscriptionService.isMatchingQuery(subscription1.q, any()) }
        verify { subscriptionService.isMatchingQuery(subscription2.q, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription1.id, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription2.id, any()) }
        verify(exactly = 2) { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should notify the subscriber that matches the geoQuery`() {
        val subscription1 = gimmeRawSubscription()
        val subscription2 = gimmeRawSubscription()
        val parsedEntity = expandJsonLdEntity(rawEntity, subscription1.contexts).toNgsiLdEntity()

        every { subscriptionService.getMatchingSubscriptions(any(), any(), any()) } answers {
            Flux.just(subscription1, subscription2)
        }
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(subscription1.id, any()) } answers { Mono.just(true) }
        every { subscriptionService.isMatchingGeoQuery(subscription2.id, any()) } answers { Mono.just(false) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        val notificationResult =
            notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity, setOf("name"))

        StepVerifier.create(notificationResult)
            .expectNextMatches {
                it.size == 1
            }
            .expectComplete()
            .verify()

        verify {
            subscriptionService.getMatchingSubscriptions(
                apiaryId.toUri(),
                listOf("https://uri.etsi.org/ngsi-ld/default-context/Apiary"),
                "name"
            )
        }
        verify { subscriptionService.isMatchingQuery(subscription1.q, any()) }
        verify { subscriptionService.isMatchingQuery(subscription2.q, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription1.id, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription2.id, any()) }
        verify(exactly = 1) { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }
        confirmVerified(subscriptionService)

        verify(1, postRequestedFor(urlPathEqualTo("/notification")))
    }

    @Test
    fun `it should return notification with data using contexts of subscription`() {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "http://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSONLD,
                    info = null
                ),
                status = null,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null
            ),
            contexts = listOf(APIC_COMPOUND_CONTEXT)
        )

        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        StepVerifier.create(
            notificationService.callSubscriber(
                subscription, apiaryId.toUri(), expandJsonLdEntity(rawEntity, subscription.contexts)
            )
        )
            .expectNextMatches {
                it.first.id == subscription.id &&
                    it.second.subscriptionId == subscription.id &&
                    it.second.data.size == 1 &&
                    it.second.data[0].containsKey("name") &&
                    it.second.data[0][JSONLD_CONTEXT] == listOf(APIC_COMPOUND_CONTEXT) &&
                    it.third
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should add link header with context of subscription`() {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "http://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSON,
                    info = null
                ),
                status = null,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null
            )
        )

        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        StepVerifier.create(
            notificationService.callSubscriber(
                subscription, apiaryId.toUri(), expandJsonLdEntity(rawEntity, subscription.contexts)
            )
        )
            .expectNextMatches {
                it.first.id == subscription.id &&
                    it.second.subscriptionId == subscription.id &&
                    it.second.data.size == 1 &&
                    !it.second.data[0].containsKey(JSONLD_CONTEXT) &&
                    it.third
            }
            .expectComplete()
            .verify()

        val link = buildContextLinkHeader(subscription.contexts[0])
        verify(
            1,
            postRequestedFor(urlPathEqualTo("/notification"))
                .withHeader(HttpHeaders.LINK, equalTo(link))
        )
    }

    @Test
    fun `it should add link header when subscription contexts size exceeds 1`() {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = emptyList(),
                endpoint = Endpoint(
                    uri = "http://localhost:8089/notification".toUri(),
                    accept = Endpoint.AcceptType.JSON,
                    info = null
                ),
                status = null,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null
            ),
            contexts = listOf(NGSILD_CORE_CONTEXT, NGSILD_CORE_CONTEXT)
        )

        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs
        every { applicationProperties.stellioUrl } answers { "http://localhost:8080" }

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        StepVerifier.create(
            notificationService.callSubscriber(
                subscription, apiaryId.toUri(), expandJsonLdEntity(rawEntity, subscription.contexts)
            )
        )
            .expectNextMatches {
                it.first.id == subscription.id &&
                    it.second.subscriptionId == subscription.id &&
                    it.second.data.size == 1 &&
                    it.third
            }
            .expectComplete()
            .verify()

        val link = buildContextLinkHeader("http://localhost:8080/ngsi-ld/v1/subscriptions/${subscription.id}/context")
        verify(
            1,
            postRequestedFor(urlPathEqualTo("/notification"))
                .withHeader(HttpHeaders.LINK, equalTo(link))
        )
    }

    @Test
    fun `it should call the subscriber`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        stubFor(
            post(urlMatching("/notification"))
                .willReturn(ok())
        )

        StepVerifier.create(
            notificationService.callSubscriber(
                subscription, apiaryId.toUri(), expandJsonLdEntity(rawEntity, subscription.contexts)
            )
        )
            .expectNextMatches {
                it.first.id == subscription.id &&
                    it.second.subscriptionId == subscription.id &&
                    it.second.data.size == 1 &&
                    it.third
            }
            .expectComplete()
            .verify()

        verify(exactly = 1) { subscriptionService.updateSubscriptionNotification(subscription, any(), true) }
        confirmVerified(subscriptionService)

        verify(1, postRequestedFor(urlPathEqualTo("/notification")))
    }

    @Test
    fun `it should call the FCM subscriber`() {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = listOf("incoming"),
                format = FormatType.KEY_VALUES,
                endpoint = Endpoint(
                    uri = "urn:embedded:firebase".toUri(),
                    accept = Endpoint.AcceptType.JSONLD,
                    info = listOf(EndpointInfo(key = "deviceToken", value = "deviceToken-value"))
                ),
                status = null,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null
            )
        )

        every { fcmService.sendMessage(any(), any(), any()) } returns "Notification sent successfully"
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }
        every { subscriptionEventService.publishNotificationCreateEvent(any(), any()) } just Runs

        StepVerifier.create(
            notificationService.callSubscriber(
                subscription,
                apiaryId.toUri(),
                expandJsonLdEntity(rawEntity, subscription.contexts)
            )
        )
            .expectNextMatches {
                it.first.id == subscription.id &&
                    it.second.subscriptionId == subscription.id &&
                    it.third
            }
            .expectComplete()
            .verify()

        verify { fcmService.sendMessage(any(), any(), "deviceToken-value") }
        verify(exactly = 1) { subscriptionService.updateSubscriptionNotification(subscription, any(), true) }
        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should not call the FCM subscriber if no fcmDeviceToken was found`() {
        val subscription = gimmeRawSubscription().copy(
            notification = NotificationParams(
                attributes = listOf("incoming"),
                format = FormatType.KEY_VALUES,
                endpoint = Endpoint(
                    uri = "urn:embedded:firebase".toUri(),
                    accept = Endpoint.AcceptType.JSONLD,
                    info = listOf(EndpointInfo(key = "unknownToken-key", value = "deviceToken-value"))
                ),
                status = null,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null
            )
        )

        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }

        StepVerifier.create(
            notificationService.callSubscriber(
                subscription,
                apiaryId.toUri(),
                expandJsonLdEntity(rawEntity, subscription.contexts)
            )
        )
            .expectNextMatches {
                it.first.id == subscription.id &&
                    it.second.subscriptionId == subscription.id &&
                    !it.third
            }
            .expectComplete()
            .verify()

        verify(exactly = 1) { subscriptionService.updateSubscriptionNotification(subscription, any(), false) }
        verify { fcmService wasNot Called }
        confirmVerified(subscriptionService)
    }
}
