package com.egm.stellio.subscription.service

import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.egm.stellio.subscription.utils.parseEntity
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@ActiveProfiles("test")
class NotificationServiceTests {

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @Autowired
    private lateinit var notificationService: NotificationService

    private lateinit var wireMockServer: WireMockServer

    private final val rawEntity = """
            {
               "id":"urn:ngsi-ld:Apiary:XYZ01",
               "type":"Apiary",
               "name":{
                  "type":"Property",
                  "value":"ApiarySophia"
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
                  "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                  "https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/ed0b0103c8b498c034d8ad367d3494d02a9ad28b/apic.jsonld",
                  "https://gist.githubusercontent.com/bobeal/4a836c81b837673b12e9db9916b1cd35/raw/82fba02005f3fc572d60744e38f6591bbaa09d6d/egm.jsonld"
               ]
            } 
        """.trimIndent()

    private val parsedEntity = parseEntity(rawEntity)

    @BeforeAll
    fun beforeAll() {
        wireMockServer = WireMockServer(wireMockConfig().port(8089))
        wireMockServer.start()
        // If not using the default port, we need to instruct explicitly the client (quite redundant)
        configureFor(8089)
    }

    @AfterAll
    fun afterAll() {
        wireMockServer.stop()
    }

    @Test
    fun `it should notify the subscriber and update the subscription`() {

        val subscription = gimmeRawSubscription()

        every { subscriptionService.getMatchingSubscriptions(any(), any()) } returns Flux.just(subscription)
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(any(), any()) } answers { Mono.just(true) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }

        stubFor(post(urlMatching("/notification"))
            .willReturn(ok()))

        val notificationResult = notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity)

        StepVerifier.create(notificationResult)
            .expectNextMatches {
                it.size == 1 &&
                    it[0].second.subscriptionId == subscription.id &&
                    it[0].second.data.size == 1 &&
                    it[0].third
            }
            .expectComplete()
            .verify()

        verify { subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Apiary:XYZ01", "https://ontology.eglobalmark.com/apic#Apiary") }
        verify { subscriptionService.isMatchingQuery(subscription.q, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription.id, any()) }
        verify { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }

        confirmVerified(subscriptionService)
    }

    @Test
    fun `it should notify the two subscribers`() {

        val subscription1 = gimmeRawSubscription()
        val subscription2 = gimmeRawSubscription()

        every { subscriptionService.getMatchingSubscriptions(any(), any()) } returns Flux.just(subscription1, subscription2)
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(any(), any()) } answers { Mono.just(true) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }

        stubFor(post(urlMatching("/notification"))
            .willReturn(ok()))

        val notificationResult = notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity)

        StepVerifier.create(notificationResult)
            .expectNextMatches {
                it.size == 2
            }
            .expectComplete()
            .verify()

        verify { subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Apiary:XYZ01", "https://ontology.eglobalmark.com/apic#Apiary") }
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

        every { subscriptionService.getMatchingSubscriptions(any(), any()) } returns Flux.just(subscription1, subscription2)
        every { subscriptionService.isMatchingQuery(any(), any()) } answers { true }
        every { subscriptionService.isMatchingGeoQuery(subscription1.id, any()) } answers { Mono.just(true) }
        every { subscriptionService.isMatchingGeoQuery(subscription2.id, any()) } answers { Mono.just(false) }
        every { subscriptionService.updateSubscriptionNotification(any(), any(), any()) } answers { Mono.just(1) }

        stubFor(post(urlMatching("/notification"))
                .willReturn(ok()))

        val notificationResult = notificationService.notifyMatchingSubscribers(rawEntity, parsedEntity)

        StepVerifier.create(notificationResult)
                .expectNextMatches {
                    it.size == 1
                }
                .expectComplete()
                .verify()

        verify { subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Apiary:XYZ01", "https://ontology.eglobalmark.com/apic#Apiary") }
        verify { subscriptionService.isMatchingQuery(subscription1.q, any()) }
        verify { subscriptionService.isMatchingQuery(subscription2.q, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription1.id, any()) }
        verify { subscriptionService.isMatchingGeoQuery(subscription2.id, any()) }
        verify(exactly = 1) { subscriptionService.updateSubscriptionNotification(any(), any(), any()) }
        confirmVerified(subscriptionService)

        verify(1, postRequestedFor(urlPathEqualTo("/notification")))
    }
}