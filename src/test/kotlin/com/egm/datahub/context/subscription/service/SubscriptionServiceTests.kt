package com.egm.datahub.context.subscription.service

import com.egm.datahub.context.subscription.model.*
import com.egm.datahub.context.subscription.utils.gimmeRawSubscription
import com.ninjasquad.springmockk.SpykBean
import io.mockk.verify
import junit.framework.TestCase.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.net.URI

@SpringBootTest
@ActiveProfiles("test")
class SubscriptionServiceTests : TimescaleBasedTests() {

    @Autowired
    private lateinit var subscriptionService: SubscriptionService

    @Autowired
    @SpykBean
    private lateinit var databaseClient: DatabaseClient

    private lateinit var subscription1Id: String
    private lateinit var subscription2Id: String

    @BeforeAll
    fun bootstrapSubscriptions() {
        val subscription1 = gimmeRawSubscription(withGeoQuery = false).copy(
            name = "Subscription 1",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beehive"),
                EntityInfo(id = null, idPattern = "urn:ngsi-ld:Beekeeper:1234*", type = "Beekeeper")
            )
        )
        subscriptionService.create(subscription1).block()
        subscription1Id = subscription1.id

        val subscription2 = gimmeRawSubscription(withGeoQuery = true).copy(
            name = "Subscription 2",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beekeeper"),
                EntityInfo(id = "urn:ngsi-ld:Beehive:1234567890", idPattern = null, type = "Beehive")
                )
        )
        subscriptionService.create(subscription2).block()
        subscription2Id = subscription2.id

        val subscription3 = gimmeRawSubscription().copy(
            name = "Subscription 3",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Apiary")
            )
        )
        subscriptionService.create(subscription3).block()
    }

    @Test
    fun `it should create a subscription insert the 3 entities info`() {
        val subscription = gimmeRawSubscription()

        val creationResult = subscriptionService.create(subscription)

        StepVerifier.create(creationResult)
            .expectComplete()
            .verify()

        // TODO this is not totally satisfying but well it's a first check that our inserts are triggered
        verify { databaseClient.execute(match<String> {
            it.startsWith("INSERT INTO subscription")
        }) }
        verify(atLeast = 3) { databaseClient.execute("INSERT INTO entity_info (id, id_pattern, type, subscription_id) VALUES (:id, :id_pattern, :type, :subscription_id)") }
        verify(atLeast = 1) { databaseClient.execute("INSERT INTO geometry_query (georel, geometry, coordinates, subscription_id) VALUES (:georel, :geometry, :coordinates, :subscription_id)") }
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info`() {

        val persistedSubscription = subscriptionService.getById(subscription1Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.name == "Subscription 1" &&
                it.description == "My beautiful subscription" &&
                it.notification.attributes == listOf("incoming") &&
                it.notification.format == NotificationParams.FormatType.KEY_VALUES &&
                it.notification.endpoint == Endpoint(URI("http://localhost:8089/notification"), Endpoint.AcceptType.JSONLD) &&
                it.entities.size == 2 &&
                it.entities.any { it.type == "Beekeeper" && it.id == null && it.idPattern == "urn:ngsi-ld:Beekeeper:1234*" } &&
                it.entities.any { it.type == "Beehive" && it.id == null && it.idPattern == null } &&
                it.geoQ == null
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and geoquery`() {

        val persistedSubscription = subscriptionService.getById(subscription2Id)

        StepVerifier.create(persistedSubscription)
                .expectNextMatches {
                    it.name == "Subscription 2" &&
                    it.description == "My beautiful subscription" &&
                    it.notification.attributes == listOf("incoming") &&
                    it.notification.format == NotificationParams.FormatType.KEY_VALUES &&
                    it.notification.endpoint == Endpoint(URI("http://localhost:8089/notification"), Endpoint.AcceptType.JSONLD) &&
                    it.entities.size == 2 &&
                    it.geoQ == GeoQuery(georel = "within", geometry = GeoQuery.GeometryType.Polygon, coordinates = "[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]")
                }
                .verifyComplete()
    }

    @Test
    fun `it should delete an existing subscription`() {
        val subscription = gimmeRawSubscription()
        subscriptionService.create(subscription).block()

        val deletionResult = subscriptionService.deleteSubscription(subscription.id).block()

        assertEquals(deletionResult, 1)
    }

    @Test
    fun `it should not delete an unknown subscription`() {
        val deletionResult = subscriptionService.deleteSubscription("urn:ngsi-ld:Subscription:UnknownSubscription").block()

        assertEquals(deletionResult, 0)
    }

    @Test
    fun `it should retrieve a subscription matching an idPattern`() {

        val persistedSubscription = subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Beekeeper:12345678", "Beekeeper")

        StepVerifier.create(persistedSubscription)
            .expectNextCount(2L)
            .verifyComplete()
    }

    @Test
    fun `it should not retrieve a subscription if idPattern does not match`() {

        val persistedSubscription = subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Beekeeper:9876543", "Beekeeper")

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.name == "Subscription 2"
            }
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching a type and not one with non matching id`() {

        val persistedSubscription = subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Beehive:ABCD", "Beehive")

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.name == "Subscription 1" &&
                    it.notification.endpoint == Endpoint(URI("http://localhost:8089/notification"), Endpoint.AcceptType.JSONLD) &&
                    it.entities.isEmpty()
            }
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching a type and an exact id`() {

        val persistedSubscription = subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Beehive:1234567890", "Beehive")

        StepVerifier.create(persistedSubscription)
            .expectNextCount(2)
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching an id`() {

        val persistedSubscription = subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Beehive:1234567890", "Beehive")

        StepVerifier.create(persistedSubscription)
            .expectNextCount(2)
            .verifyComplete()
    }

    @Test
    fun `it should not retrieve a subscription if type does not match`() {

        val persistedSubscription = subscriptionService.getMatchingSubscriptions("urn:ngsi-ld:Sensor:1234567890", "Sensor")

        StepVerifier.create(persistedSubscription)
            .expectComplete()
    }

    @Test
    fun `it should update a subscription with a notification result`() {

        val persistedSubscription = subscriptionService.getById(subscription1Id).block()!!
        val notification = Notification(subscriptionId = subscription1Id, data = listOf("entity"))

        val updateResult = subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)
            .then(subscriptionService.getById(subscription1Id))

        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.id == subscription1Id &&
                    it.notification.status == NotificationParams.StatusType.OK
                    it.notification.timesSent == 1 &&
                    it.notification.lastNotification != null &&
                    it.notification.lastSuccess != null &&
                    it.notification.lastFailure == null
            }
            .verifyComplete()
    }
}
