package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EventType
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import com.jayway.jsonpath.JsonPath.read
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.verify
import junit.framework.TestCase.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.net.URI

@SpringBootTest
@ActiveProfiles("test")
class SubscriptionServiceTests : TimescaleBasedTests() {

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String = ""

    @Autowired
    private lateinit var subscriptionService: SubscriptionService

    @Autowired
    @SpykBean
    private lateinit var databaseClient: DatabaseClient

    /**
     * As Spring's ApplicationEventPublisher is not easily mockable (https://github.com/spring-projects/spring-framework/issues/18907),
     * we are directly mocking the event listener to check it receives what is expected
     */
    @MockkBean(relaxed = true)
    private lateinit var subscriptionsEventsListener: SubscriptionsEventsListener

    private lateinit var subscription1Id: String
    private lateinit var subscription2Id: String
    private lateinit var subscription3Id: String
    private lateinit var subscription4Id: String

    private val entity = ClassPathResource("/ngsild/aquac/FeedingService.json").inputStream.readBytes().toString(Charsets.UTF_8)

    @BeforeAll
    fun bootstrapSubscriptions() {

        every { subscriptionsEventsListener.handleSubscriptionEvent(any()) } just Runs

        val subscription1 = gimmeRawSubscription(withGeoQuery = false, withEndpointInfo = false).copy(
            name = "Subscription 1",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beehive"),
                EntityInfo(id = null, idPattern = "urn:ngsi-ld:Beekeeper:1234*", type = "Beekeeper")
            )
        )
        subscriptionService.create(subscription1).block()
        subscription1Id = subscription1.id

        val subscription2 = gimmeRawSubscription(withGeoQuery = true, withEndpointInfo = true).copy(
            name = "Subscription 2",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beekeeper"),
                EntityInfo(id = "urn:ngsi-ld:Beehive:1234567890", idPattern = null, type = "Beehive")
            )
        )
        subscriptionService.create(subscription2).block()
        subscription2Id = subscription2.id

        val subscription3 = gimmeRawSubscription(withQuery = true, withEndpointInfo = false).copy(
            name = "Subscription 3",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Apiary")
            )
        )
        subscriptionService.create(subscription3).block()
        subscription3Id = subscription3.id

        val subscription4 = gimmeRawSubscription(withQuery = true, withGeoQuery = true, withEndpointInfo = false).copy(
            name = "Subscription 4",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beehive")
            )
        )
        subscriptionService.create(subscription4).block()
        subscription4Id = subscription4.id
    }

    @Test
    fun `it should create a subscription insert the 3 entities info`() {
        val subscription = gimmeRawSubscription(withEndpointInfo = false).copy(
            entities = setOf(
                EntityInfo(id = "urn:ngsi-ld:FishContainment:1234567890", idPattern = null, type = "FishContainment"),
                EntityInfo(id = null, idPattern = "urn:ngsi-ld:FishContainment:*", type = "FishContainment")
            )
        )
        every { subscriptionsEventsListener.handleSubscriptionEvent(any()) } just Runs

        val creationResult = subscriptionService.create(subscription)

        StepVerifier.create(creationResult)
            .expectNext(2)
            .expectComplete()
            .verify()

        // TODO this is not totally satisfying but well it's a first check that our inserts are triggered
        verify { databaseClient.execute(match<String> {
            it.startsWith("INSERT INTO subscription")
        }) }
        verify(atLeast = 2) { databaseClient.execute("INSERT INTO entity_info (id, id_pattern, type, subscription_id) VALUES (:id, :id_pattern, :type, :subscription_id)") }
        verify(atLeast = 1) { databaseClient.execute("INSERT INTO geometry_query (georel, geometry, coordinates, subscription_id) VALUES (:georel, :geometry, :coordinates, :subscription_id)") }
        verify(timeout = 1000, exactly = 1) { subscriptionsEventsListener.handleSubscriptionEvent(match { entityEvent ->
            entityEvent.entityType == "Subscription" &&
            entityEvent.entityId == subscription.id &&
            entityEvent.operationType == EventType.CREATE &&
            entityEvent.payload != null &&
            entityEvent.updatedEntity == null
        }) }
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
                it.notification.endpoint == Endpoint(URI("http://localhost:8089/notification"), Endpoint.AcceptType.JSONLD, null) &&
                it.entities.size == 2 &&
                it.entities.any { it.type == "Beekeeper" && it.id == null && it.idPattern == "urn:ngsi-ld:Beekeeper:1234*" } &&
                it.entities.any { it.type == "Beehive" && it.id == null && it.idPattern == null } &&
                it.geoQ == null
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and query`() {

        val persistedSubscription = subscriptionService.getById(subscription3Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.name == "Subscription 3" &&
                it.description == "My beautiful subscription" &&
                it.q == "speed>50;foodName==dietary fibres" &&
                it.notification.attributes == listOf("incoming") &&
                it.notification.format == NotificationParams.FormatType.KEY_VALUES &&
                it.notification.endpoint == Endpoint(URI("http://localhost:8089/notification"), Endpoint.AcceptType.JSONLD, null) &&
                it.entities.size == 1
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and geoquery and endpoint info`() {

        val persistedSubscription = subscriptionService.getById(subscription2Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.name == "Subscription 2" &&
                it.description == "My beautiful subscription" &&
                it.notification.attributes == listOf("incoming") &&
                it.notification.format == NotificationParams.FormatType.KEY_VALUES &&
                it.notification.endpoint == Endpoint(
                    URI("http://localhost:8089/notification"),
                    Endpoint.AcceptType.JSONLD,
                    listOf(EndpointInfo("Authorization-token", "Authorization-token-value"))
                ) &&
                it.entities.size == 2 &&
                it.geoQ == GeoQuery(georel = "within", geometry = GeoQuery.GeometryType.Polygon, coordinates = "[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]")
            }
            .verifyComplete()
    }

    @Test
    fun `it should delete an existing subscription`() {
        val subscription = gimmeRawSubscription()

        every { subscriptionsEventsListener.handleSubscriptionEvent(any()) } just Runs

        subscriptionService.create(subscription).block()

        val deletionResult = subscriptionService.delete(subscription.id).block()

        verify(timeout = 1000, exactly = 1) { subscriptionsEventsListener.handleSubscriptionEvent(match { entityEvent ->
            entityEvent.entityType == "Subscription" &&
            entityEvent.entityId == subscription.id &&
            entityEvent.operationType == EventType.DELETE &&
            entityEvent.payload == null &&
            entityEvent.updatedEntity == null
        }) }

        assertEquals(deletionResult, 1)
    }

    @Test
    fun `it should not delete an unknown subscription`() {
        val deletionResult = subscriptionService.delete("urn:ngsi-ld:Subscription:UnknownSubscription").block()

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
            .verify()
    }

    @Test
    fun `it should update a subscription `() {

        val parsedInput = Pair(mapOf("name" to "My Subscription Updated",
                "description" to "My beautiful subscription has been updated",
                "q" to "foodQuantity>=150",
                "geoQ" to mapOf("georel" to "equals", "geometry" to "Point", "coordinates" to "[100.0, 0.0]")), listOf(apicContext))

        every { subscriptionsEventsListener.handleSubscriptionEvent(any()) } just Runs

        subscriptionService.update(subscription4Id, parsedInput).block()
        val updateResult = subscriptionService.getById(subscription4Id)

        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.name == "My Subscription Updated" &&
                it.description == "My beautiful subscription has been updated" &&
                it.q == "foodQuantity>=150" &&
                it.geoQ!!.georel == "equals" &&
                it.geoQ!!.geometry.name == "Point" &&
                it.geoQ!!.coordinates == "[100.0, 0.0]"
            }
            .verifyComplete()

        verify(timeout = 1000, exactly = 1) { subscriptionsEventsListener.handleSubscriptionEvent(match { entityEvent ->
            entityEvent.entityType == "Subscription" &&
            entityEvent.entityId == subscription4Id &&
            entityEvent.operationType == EventType.UPDATE &&
            entityEvent.payload != null &&
            read(entityEvent.updatedEntity, "$.name") as String == "My Subscription Updated" &&
            read(entityEvent.updatedEntity, "$.description") as String == "My beautiful subscription has been updated" &&
            read(entityEvent.updatedEntity, "$.q") as String == "foodQuantity>=150"
        }) }
    }

    @Test
    fun `it should update a subscription notification`() {

        val parsedInput = mapOf("attributes" to listOf("outgoing"),
            "format" to "keyValues",
            "endpoint" to mapOf("accept" to "application/ld+json",
                "uri" to "http://localhost:8080",
                "info" to listOf(
                    mapOf("key" to "Authorization-token", "value" to "Authorization-token-newValue")
                )))

        subscriptionService.updateNotification(subscription4Id, parsedInput, listOf(apicContext)).block()
        val updateResult = subscriptionService.getById(subscription4Id)

        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.notification.attributes == listOf("https://ontology.eglobalmark.com/apic#outgoing") &&
                it.notification.format.name == "KEY_VALUES" &&
                it.notification.endpoint.accept.name == "JSONLD" &&
                it.notification.endpoint.uri.toString() == "http://localhost:8080" &&
                it.notification.endpoint.info == listOf(
                    EndpointInfo("Authorization-token", "Authorization-token-newValue")
                ) &&
                it.notification.endpoint.info!!.size == 1
            }
            .verifyComplete()
    }

    @Test
    fun `it should update a subscription entities`() {

        val parsedInput = listOf(
            mapOf("id" to "urn:ngsi-ld:Beehive:123",
                "type" to "Beehive"),
            mapOf("idPattern" to "urn:ngsi-ld:Beehive:12*",
                "type" to "Beehive")
        )

        subscriptionService.updateEntities(subscription4Id, parsedInput, listOf(apicContext)).block()
        val updateResult = subscriptionService.getById(subscription4Id)

        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.entities.contains(EntityInfo(id = "urn:ngsi-ld:Beehive:123", idPattern = null, type = "https://uri.etsi.org/ngsi-ld/default-context/Beehive")) &&
                it.entities.contains(EntityInfo(id = null, idPattern = "urn:ngsi-ld:Beehive:12*", type = "https://uri.etsi.org/ngsi-ld/default-context/Beehive")) &&
                it.entities.size == 2
            }
            .verifyComplete()
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
                it.notification.status == NotificationParams.StatusType.OK &&
                it.notification.timesSent == 1 &&
                it.notification.lastNotification != null &&
                it.notification.lastSuccess != null &&
                it.notification.lastFailure == null
            }
            .verifyComplete()
    }

    @Test
    fun `it should return true if query is null`() {
        val query = null
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, true)
    }

    @Test
    fun `it should return false if query is invalid`() {
        val query = "foodQuantity.invalidAttribute>=150"
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, false)
    }

    @Test
    fun `it should return true if entity matches query`() {
        val query = "foodQuantity<150"
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, true)
    }

    @Test
    fun `it should return false if entity don't matches query`() {
        val query = "foodQuantity>=150"
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, false)
    }

    @Test
    fun `it should support multiple predicates query`() {
        val query = "(foodQuantity<=150;foodName==\"dietary fibres\");executes==\"urn:ngsi-ld:Feeder:018z5\""
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, true)
    }

    @Test
    fun `it should support multiple predicates query with logical operator`() {
        val query = "foodQuantity>150;executes.createdAt==\"2018-11-26T21:32:52+02:00\""
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, false)
    }

    @Test
    fun `it should support multiple predicates query with or logical operator`() {
        val query = "foodQuantity>150|executes.createdAt==\"2018-11-26T21:32:52+02:00\""
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, true)
    }

    @Test
    fun `it should support boolean value type`() {
        val query = "foodName[isHealthy]!=false"
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, true)
    }
}
