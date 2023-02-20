package com.egm.stellio.subscription.service

import arrow.core.Some
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import com.egm.stellio.subscription.model.NotificationParams.StatusType
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.support.WithTimescaleContainer
import com.egm.stellio.subscription.utils.ParsingUtils
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.ninjasquad.springmockk.SpykBean
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import reactor.test.StepVerifier
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class SubscriptionServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var subscriptionService: SubscriptionService

    @Autowired
    @SpykBean
    private lateinit var databaseClient: DatabaseClient

    private val mockUserSub = Some(UUID.randomUUID().toString())

    private lateinit var subscription1Id: URI
    private lateinit var subscription2Id: URI
    private lateinit var subscription3Id: URI
    private lateinit var subscription4Id: URI
    private lateinit var subscription5Id: URI
    private lateinit var subscription6Id: URI

    private val entity =
        ClassPathResource("/ngsild/aquac/FeedingService.json").inputStream.readBytes().toString(Charsets.UTF_8)

    @BeforeAll
    fun bootstrapSubscriptions() {
        createSubscription1()
        createSubscription2()
        createSubscription3()
        createSubscription4()
        createSubscription5()
        createSubscription6()
    }

    private fun createSubscription(subscription: Subscription): URI {
        runBlocking {
            subscriptionService.create(subscription, mockUserSub)
        }
        return subscription.id
    }

    private fun createSubscription1() {
        val subscription = gimmeRawSubscription(
            withQueryAndGeoQuery = Pair(first = true, second = false),
            withEndpointInfo = false,
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming"))
        ).copy(
            subscriptionName = "Subscription 1",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beehive"),
                EntityInfo(id = null, idPattern = "urn:ngsi-ld:Beekeeper:1234*", type = "Beekeeper")
            )
        )
        subscription1Id = createSubscription(subscription)
    }

    private fun createSubscription2() {
        val subscription = gimmeRawSubscription(
            withEndpointInfo = true,
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming"))
        ).copy(
            subscriptionName = "Subscription 2",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beekeeper"),
                EntityInfo(id = "urn:ngsi-ld:Beehive:1234567890".toUri(), idPattern = null, type = "Beehive")
            ),
            expiresAt = Instant.now().atZone(ZoneOffset.UTC).plusDays(1)
        )
        subscription2Id = createSubscription(subscription)
    }

    private fun createSubscription3() {
        val subscription = gimmeRawSubscription(
            withEndpointInfo = false,
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming"))
        ).copy(
            subscriptionName = "Subscription 3",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Apiary")
            ),
            isActive = false
        )
        subscription3Id = createSubscription(subscription)
    }

    private fun createSubscription4() {
        val subscription = gimmeRawSubscription(
            withEndpointInfo = false,
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming"))
        ).copy(
            subscriptionName = "Subscription 4",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beehive")
            ),
            isActive = false,
            watchedAttributes = listOf("incoming", "outgoing")
        )
        subscription4Id = createSubscription(subscription)
    }

    private fun createSubscription5() {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming"))
        ).copy(
            subscriptionName = "Subscription 5",
            entities = setOf(
                EntityInfo(id = "urn:ngsi-ld:smartDoor:77".toUri(), idPattern = null, type = "smartDoor")
            ),
            isActive = true,
            expiresAt = null
        )
        subscription5Id = createSubscription(subscription)
    }

    private fun createSubscription6() {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming"))
        ).copy(
            subscriptionName = "Subscription 6",
            entities = setOf(
                EntityInfo(id = "urn:ngsi-ld:smartDoor:88".toUri(), idPattern = null, type = "smartDoor")
            ),
            isActive = false,
            expiresAt = ZonedDateTime.parse("2012-08-12T08:33:38Z")
        )
        subscription6Id = createSubscription(subscription)
    }

    @Test
    fun `it should not retrieve an expired subscription matching an id`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:smartDoor:88".toUri(),
                listOf("smartDoor"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextCount(0)
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching an id when expired date is not given`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:smartDoor:77".toUri(),
                listOf("smartDoor"),
                "incoming"
            )
        StepVerifier.create(persistedSubscription)
            .expectNextCount(1L)
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching an id when expired date is in the future`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf("Beehive"),
                "incoming"
            )
        StepVerifier.create(persistedSubscription)
            .expectNextCount(2L)
            .verifyComplete()
    }

    @Test
    fun `it should not allow a subscription with an empty id`() = runTest {
        val payload = mapOf(
            "id" to "",
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = ParsingUtils.parseSubscription(payload, emptyList()).shouldSucceedAndResult()
        subscriptionService.validateNewSubscription(subscription)
            .shouldFail {
                it is BadRequestDataException &&
                    it.message == "The supplied identifier was expected to be an URI but it is not: "
            }
    }

    @Test
    fun `it should not allow a subscription with an invalid id`() = runTest {
        val payload = mapOf(
            "id" to "invalidId",
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = ParsingUtils.parseSubscription(payload, emptyList()).shouldSucceedAndResult()
        subscriptionService.validateNewSubscription(subscription)
            .shouldFail {
                it is BadRequestDataException &&
                    it.message == "The supplied identifier was expected to be an URI but it is not: invalidId"
            }
    }

    @Test
    fun `it should not allow a subscription with an invalid idPattern`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE, "idPattern" to "[")),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = ParsingUtils.parseSubscription(payload, emptyList()).shouldSucceedAndResult()
        subscriptionService.validateNewSubscription(subscription)
            .shouldFail {
                it is BadRequestDataException &&
                    it.message == "Invalid value for idPattern: ["
            }
    }

    @Test
    fun `it should create a subscription and insert the 3 entities info`() = runTest {
        val subscription = gimmeRawSubscription(withEndpointInfo = false).copy(
            entities = setOf(
                EntityInfo(
                    id = "urn:ngsi-ld:FishContainment:1234567890".toUri(),
                    idPattern = null,
                    type = "FishContainment"
                ),
                EntityInfo(id = null, idPattern = "urn:ngsi-ld:FishContainment:*", type = "FishContainment")
            )
        )

        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        // TODO this is not totally satisfying but well it's a first check that our inserts are triggered
        verify {
            databaseClient.sql(
                match<String> {
                    it.startsWith("INSERT INTO subscription")
                }
            )
        }
        verify(atLeast = 2) {
            databaseClient.sql(
                match<String> {
                    """
                    INSERT INTO entity_info (id, id_pattern, type, subscription_id)
                    VALUES (:id, :id_pattern, :type, :subscription_id)
                    """.matchContent(it)
                }
            )
        }
        verify(atLeast = 1) {
            databaseClient.sql(
                match<String> {
                    """
                    INSERT INTO geometry_query (
                        georel, geometry, coordinates, pgis_geometry, geoproperty, subscription_id
                    )
                    VALUES (
                        :georel, :geometry, :coordinates, ST_GeomFromText(:wkt_coordinates), :geoproperty, 
                        :subscription_id
                    )
                    """.matchContent(it)
                }
            )
        }
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info`() {
        val persistedSubscription = subscriptionService.getById(subscription1Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 1" &&
                    it.description == "My beautiful subscription" &&
                    it.notification.attributes == listOf("incoming") &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint ==
                    Endpoint(
                        URI("http://localhost:8089/notification"),
                        Endpoint.AcceptType.JSONLD,
                        null
                    ) &&
                    it.entities.size == 2 &&
                    it.entities.any { entityInfo ->
                        entityInfo.type == "Beekeeper" &&
                            entityInfo.id == null &&
                            entityInfo.idPattern == "urn:ngsi-ld:Beekeeper:1234*"
                    } &&
                    it.entities.any { entityInfo ->
                        entityInfo.type == "Beehive" &&
                            entityInfo.id == null &&
                            entityInfo.idPattern == null
                    } &&
                    it.geoQ == null
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and query`() {
        val persistedSubscription = subscriptionService.getById(subscription3Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 3" &&
                    it.description == "My beautiful subscription" &&
                    it.q == "speed>50;foodName==dietary fibres" &&
                    it.notification.attributes == listOf("incoming") &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint == Endpoint(
                    URI("http://localhost:8089/notification"),
                    Endpoint.AcceptType.JSONLD,
                    null
                ) &&
                    it.entities.size == 1
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and geoquery and endpoint info`() {
        val persistedSubscription = subscriptionService.getById(subscription2Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 2" &&
                    it.description == "My beautiful subscription" &&
                    it.notification.attributes == listOf("incoming") &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint == Endpoint(
                    URI("http://localhost:8089/notification"),
                    Endpoint.AcceptType.JSONLD,
                    listOf(EndpointInfo("Authorization-token", "Authorization-token-value"))
                ) &&
                    it.entities.size == 2 &&
                    it.geoQ != null &&
                    it.geoQ!!.georel == "within" &&
                    it.geoQ!!.geometry == "Polygon" &&
                    it.geoQ!!.geoproperty == "https://uri.etsi.org/ngsi-ld/location" &&
                    it.geoQ!!.coordinates == "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]"
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and active status`() {
        val persistedSubscription = subscriptionService.getById(subscription2Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 2" &&
                    it.description == "My beautiful subscription" &&
                    it.notification.attributes == listOf("incoming") &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint == Endpoint(
                    URI("http://localhost:8089/notification"),
                    Endpoint.AcceptType.JSONLD,
                    listOf(EndpointInfo("Authorization-token", "Authorization-token-value"))
                ) &&
                    it.entities.size == 2 &&
                    it.isActive
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and inactive status`() {
        val persistedSubscription = subscriptionService.getById(subscription4Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 4" &&
                    it.description == "My beautiful subscription" &&
                    it.notification.attributes == listOf("incoming") &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint == Endpoint(
                    URI("http://localhost:8089/notification"),
                    Endpoint.AcceptType.JSONLD,
                    null
                ) &&
                    it.entities.size == 1 &&
                    !it.isActive
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and null value for watched attributes`() {
        val persistedSubscription = subscriptionService.getById(subscription2Id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 2" &&
                    it.description == "My beautiful subscription" &&
                    it.entities.size == 2 &&
                    it.watchedAttributes == null
            }
            .verifyComplete()
    }

    @Test
    fun `it should load and fill a persisted subscription with the correct format for temporal values`() = runTest {
        val createdAt = Instant.now().truncatedTo(ChronoUnit.MICROS).atZone(ZoneOffset.UTC)
        val subscription = gimmeRawSubscription().copy(
            createdAt = createdAt,
            entities = setOf(
                EntityInfo(id = "urn:ngsi-ld:smartDoor:77".toUri(), idPattern = null, type = "smartDoor")
            )
        )
        val notifiedAt = Instant.now().truncatedTo(ChronoUnit.MICROS).atZone(ZoneOffset.UTC)

        subscriptionService.create(subscription, mockUserSub)
        subscriptionService.updateSubscriptionNotification(
            subscription,
            Notification(subscriptionId = subscription.id, notifiedAt = notifiedAt, data = emptyList()),
            true
        ).block()

        val persistedSubscription = subscriptionService.getById(subscription.id)

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.notification.lastNotification != null &&
                    it.notification.lastNotification!!.isEqual(notifiedAt) &&
                    it.notification.lastSuccess != null &&
                    it.notification.lastSuccess!!.isEqual(notifiedAt) &&
                    it.createdAt.isEqual(createdAt)
            }
            .verifyComplete()
    }

    @Test
    fun `it should delete an existing subscription`() = runTest {
        val subscription = gimmeRawSubscription()

        subscriptionService.create(subscription, mockUserSub)

        StepVerifier.create(subscriptionService.delete(subscription.id))
            .expectNext(1)
            .verifyComplete()
    }

    @Test
    fun `it should not delete an unknown subscription`() {
        StepVerifier.create(subscriptionService.delete("urn:ngsi-ld:Subscription:UnknownSubscription".toUri()))
            .expectNext(0)
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching an idPattern`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:12345678".toUri(),
                listOf("Beekeeper"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextCount(2L)
            .verifyComplete()
    }

    @Test
    fun `it should not retrieve a subscription if idPattern does not match`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:9876543".toUri(),
                listOf("Beekeeper"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 2"
            }
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching a type and not one with non matching id`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:ABCD".toUri(),
                listOf("Beehive"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 1" &&
                    it.notification.endpoint == Endpoint(
                    URI("http://localhost:8089/notification"),
                    Endpoint.AcceptType.JSONLD
                ) &&
                    it.entities.isEmpty()
            }
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching a type and an exact id`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf("Beehive"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextCount(2)
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription matching an id`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf("Beehive"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextCount(1)
            .verifyComplete()
    }

    @Test
    fun `it should not retrieve a subscription if type does not match`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Sensor:1234567890".toUri(),
                listOf("Sensor"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve an activated subscription matching an id`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:smartDoor:77".toUri(),
                listOf("smartDoor"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextMatches {
                it.subscriptionName == "Subscription 5"
            }
            .verifyComplete()
    }

    @Test
    fun `it should not retrieve a deactivated subscription matching an id`() {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:smartDoor:88".toUri(),
                listOf("smartDoor"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextCount(0)
            .verifyComplete()
    }

    @Test
    fun `it should retrieve a subscription if watched attributes is null`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            subscriptionName = "My subscription",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "BeeHive")
            ),
            watchedAttributes = null
        )

        subscriptionService.create(subscription, mockUserSub)

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf("BeeHive"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextCount(1)
            .verifyComplete()

        subscriptionService.delete(subscription.id).block()
    }

    @Test
    fun `it should retrieve a subscription if watched attributes contains at least one of the updated attributes`() =
        runTest {
            val subscription = gimmeRawSubscription().copy(
                subscriptionName = "My subscription",
                entities = setOf(
                    EntityInfo(id = null, idPattern = null, type = "BeeHive")
                ),
                watchedAttributes = listOf("incoming", "outgoing", "temperature")
            )

            subscriptionService.create(subscription, mockUserSub)

            val persistedSubscription =
                subscriptionService.getMatchingSubscriptions(
                    "urn:ngsi-ld:Beehive:1234567890".toUri(),
                    listOf("BeeHive"),
                    "incoming"
                )

            StepVerifier.create(persistedSubscription)
                .expectNextCount(1)
                .verifyComplete()

            subscriptionService.delete(subscription.id).block()
        }

    @Test
    fun `it should not retrieve a subscription if watched attributes do not match any updated attributes`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            subscriptionName = "My subscription",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "BeeHive")
            ),
            watchedAttributes = listOf("outgoing", "temperature")
        )

        subscriptionService.create(subscription, mockUserSub)

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf("BeeHive"),
                "incoming"
            )

        StepVerifier.create(persistedSubscription)
            .expectNextCount(0)
            .verifyComplete()

        subscriptionService.delete(subscription.id).block()
    }

    @Test
    fun `it should update a subscription`() = runTest {
        val parsedInput = mapOf(
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "subscriptionName" to "My Subscription Updated",
            "description" to "My beautiful subscription has been updated",
            "q" to "foodQuantity>=150",
            "geoQ" to mapOf(
                "georel" to "equals",
                "geometry" to "Point",
                "coordinates" to "[100.0, 0.0]",
                "geoproperty" to "https://uri.etsi.org/ngsi-ld/observationSpace"
            )
        )

        subscriptionService.update(subscription4Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
        val updateResult = subscriptionService.getById(subscription4Id)

        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.subscriptionName == "My Subscription Updated" &&
                    it.description == "My beautiful subscription has been updated" &&
                    it.q == "foodQuantity>=150" &&
                    it.geoQ!!.georel == "equals" &&
                    it.geoQ!!.geometry == "Point" &&
                    it.geoQ!!.coordinates == "[100.0, 0.0]" &&
                    it.geoQ!!.geoproperty == "https://uri.etsi.org/ngsi-ld/observationSpace"
            }
            .verifyComplete()
    }

    @Test
    fun `it should update a subscription notification`() = runTest {
        val parsedInput = mapOf(
            "attributes" to listOf("outgoing"),
            "format" to "keyValues",
            "endpoint" to mapOf(
                "accept" to "application/ld+json",
                "uri" to "http://localhost:8080",
                "info" to listOf(
                    mapOf("key" to "Authorization-token", "value" to "Authorization-token-newValue")
                )
            )
        )

        subscriptionService.updateNotification(subscription4Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
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
    fun `it should update a subscription entities`() = runTest {
        val parsedInput = listOf(
            mapOf(
                "id" to "urn:ngsi-ld:Beehive:123",
                "type" to "Beehive"
            ),
            mapOf(
                "idPattern" to "urn:ngsi-ld:Beehive:12*",
                "type" to "Beehive"
            )
        )

        subscriptionService.updateEntities(subscription4Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
        val updateResult = subscriptionService.getById(subscription4Id)

        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.entities.contains(
                    EntityInfo(
                        id = "urn:ngsi-ld:Beehive:123".toUri(),
                        idPattern = null,
                        type = "https://uri.etsi.org/ngsi-ld/default-context/Beehive"
                    )
                ) &&
                    it.entities.contains(
                        EntityInfo(
                            id = null,
                            idPattern = "urn:ngsi-ld:Beehive:12*",
                            type = "https://uri.etsi.org/ngsi-ld/default-context/Beehive"
                        )
                    ) &&
                    it.entities.size == 2
            }
            .verifyComplete()
    }

    @Test
    fun `it should activate a subscription`() = runTest {
        subscriptionService.update(
            subscription3Id,
            mapOf("type" to NGSILD_SUBSCRIPTION_TERM, "isActive" to true),
            listOf(APIC_COMPOUND_CONTEXT)
        )
        val updateResult = subscriptionService.getById(subscription3Id)
        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.isActive && it.modifiedAt != null
            }
            .verifyComplete()
    }

    @Test
    fun `it should deactivate a subscription`() = runTest {
        subscriptionService.update(
            subscription1Id,
            mapOf("type" to NGSILD_SUBSCRIPTION_TERM, "isActive" to false),
            listOf(APIC_COMPOUND_CONTEXT)
        )
        val updateResult = subscriptionService.getById(subscription1Id)

        StepVerifier.create(updateResult)
            .expectNextMatches {
                !it.isActive && it.modifiedAt != null
            }
            .verifyComplete()
    }

    @Test
    fun `it should update a subscription watched attributes`() = runTest {
        val parsedInput = mapOf(
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "watchedAttributes" to arrayListOf("incoming", "temperature")
        )

        subscriptionService.update(subscription5Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
        val updateResult = subscriptionService.getById(subscription5Id)

        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.watchedAttributes!! == listOf("incoming", "temperature") &&
                    it.modifiedAt != null
            }
            .verifyComplete()
    }

    @Test
    fun `it should throw a BadRequestData exception if the subscription has an unknown attribute`() = runTest {
        val parsedInput = mapOf("unknownAttribute" to "unknownValue")

        subscriptionService.update(subscription5Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
            .shouldFail { it is BadRequestDataException }
    }

    @Test
    fun `it should throw a NotImplemented exception if the subscription has an unsupported attribute`() = runTest {
        val parsedInput = mapOf("throttling" to "someValue")

        subscriptionService.update(subscription5Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
            .shouldFail { it is NotImplementedException }
    }

    @Test
    fun `it should update a subscription with a notification result`() {
        val persistedSubscription = subscriptionService.getById(subscription1Id).block()!!
        val notification = Notification(subscriptionId = subscription1Id, data = emptyList())

        val updateResult =
            subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)
                .then(subscriptionService.getById(subscription1Id))

        StepVerifier.create(updateResult)
            .expectNextMatches {
                it.id == subscription1Id &&
                    it.notification.status == StatusType.OK &&
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
    fun `it should support single quoted predicates query`() {
        val query = "(foodQuantity<=150;foodName==\"dietary fibres\");executes=='urn:ngsi-ld:Feeder:018z5'"
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, true)
    }

    @Test
    fun `it should support multiple predicates query with logical operator`() {
        val query = "foodQuantity>150;executes.createdAt==\"2018-11-26T21:32:52.98601Z\""
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, false)
    }

    @Test
    fun `it should support multiple predicates query with or logical operator`() {
        val query = "foodQuantity>150|executes.createdAt==\"2018-11-26T21:32:52.98601Z\""
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, true)
    }

    @Test
    fun `it should support boolean value type`() {
        val query = "foodName[isHealthy]!=false"
        val res = subscriptionService.isMatchingQuery(query, entity)

        assertEquals(res, true)
    }

    @Test
    fun `it should return all subscriptions whose 'timeInterval' is reached `() {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming")),
            timeInterval = 500
        ).copy(
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beehive")
            )
        )

        val subscriptionId = createSubscription(subscription)

        val subscription2 = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming")),
            timeInterval = 5000
        ).copy(
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beekeeper")
            )
        )
        val subscriptionId2 = createSubscription(subscription2)

        val persistedSubscription = subscriptionService.getById(subscriptionId).block()!!
        val notification = Notification(subscriptionId = subscriptionId, data = emptyList())

        runBlocking {
            subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)
                .awaitFirst()

            val listSubscription = subscriptionService.getRecurringSubscriptionsToNotify()
            assertEquals(1, listSubscription.size)
        }

        subscriptionService.delete(subscriptionId).block()
        subscriptionService.delete(subscriptionId2).block()
    }

    @Test
    fun `it should return all subscriptions whose 'timeInterval' is reached with a time of 5s`() {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming")),
            timeInterval = 1
        ).copy(
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beehive")
            )
        )

        val subscriptionId1 = createSubscription(subscription)

        val subscription2 = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf("incoming")),
            timeInterval = 5000
        ).copy(
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beekeeper")
            )
        )
        val subscriptionId2 = createSubscription(subscription2)

        val persistedSubscription = subscriptionService.getById(subscriptionId1).block()!!
        val notification = Notification(subscriptionId = subscriptionId1, data = emptyList())

        val persistedSubscription2 = subscriptionService.getById(subscriptionId2).block()!!
        val notification2 = Notification(subscriptionId = subscriptionId2, data = emptyList())

        runBlocking {
            subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)
                .awaitFirst()

            subscriptionService.updateSubscriptionNotification(persistedSubscription2, notification2, true)
                .awaitFirst()

            delay(5000)
            val listSubscription = subscriptionService.getRecurringSubscriptionsToNotify()
            assertEquals(1, listSubscription.size)
        }

        subscriptionService.delete(subscriptionId1).block()
        subscriptionService.delete(subscriptionId2).block()
    }
}
