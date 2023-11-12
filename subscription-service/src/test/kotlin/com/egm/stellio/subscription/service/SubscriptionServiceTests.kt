package com.egm.stellio.subscription.service

import arrow.core.Some
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import com.egm.stellio.subscription.model.NotificationParams.StatusType
import com.egm.stellio.subscription.model.NotificationTrigger.*
import com.egm.stellio.subscription.support.WithTimescaleContainer
import com.egm.stellio.subscription.utils.ParsingUtils
import com.egm.stellio.subscription.utils.gimmeSubscriptionFromMembers
import com.egm.stellio.subscription.utils.loadAndDeserializeSubscription
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.time.Duration

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class SubscriptionServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var subscriptionService: SubscriptionService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val mockUserSub = Some(UUID.randomUUID().toString())

    private val entity =
        ClassPathResource("/ngsild/aquac/FeedingService.json").inputStream.readBytes().toString(Charsets.UTF_8)

    private lateinit var jsonldEntity: JsonLdEntity

    @BeforeAll
    fun loadTestEntity() {
        runBlocking {
            jsonldEntity = JsonLdUtils.expandJsonLdEntity(entity, listOf(APIC_COMPOUND_CONTEXT))
        }
    }

    @AfterEach
    fun deleteSubscriptions() {
        r2dbcEntityTemplate.delete(Subscription::class.java)
            .all()
            .block()
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
            .shouldFailWith {
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
            .shouldFailWith {
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
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Invalid idPattern found in subscription"
            }
    }

    @Test
    fun `it should not allow a subscription without entities and watchedAttributes`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = ParsingUtils.parseSubscription(payload, emptyList()).shouldSucceedAndResult()
        subscriptionService.validateNewSubscription(subscription)
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "At least one of entities or watchedAttributes shall be present"
            }
    }

    @Test
    fun `it should not allow a subscription with timeInterval and watchedAttributes`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "timeInterval" to 10,
            "watchedAttributes" to listOf(INCOMING_COMPACT_PROPERTY),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = ParsingUtils.parseSubscription(payload, emptyList()).shouldSucceedAndResult()
        subscriptionService.validateNewSubscription(subscription)
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "You can't use 'timeInterval' in conjunction with 'watchedAttributes'"
            }
    }

    @Test
    fun `it should not allow a subscription with a negative timeInterval`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "watchedAttributes" to listOf(INCOMING_COMPACT_PROPERTY),
            "timeInterval" to -10,
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = ParsingUtils.parseSubscription(payload, emptyList()).shouldSucceedAndResult()
        subscriptionService.validateNewSubscription(subscription)
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "The value of 'timeInterval' must be greater than zero (int)"
            }
    }

    @Test
    fun `it should not allow a subscription with an expiresAt in the past`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "watchedAttributes" to listOf(INCOMING_COMPACT_PROPERTY),
            "expiresAt" to ngsiLdDateTime().minusDays(1),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = ParsingUtils.parseSubscription(payload, emptyList()).shouldSucceedAndResult()
        subscriptionService.validateNewSubscription(subscription)
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "'expiresAt' must be in the future"
            }
    }

    @Test
    fun `it should not allow a subscription with an unknown notification trigger`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_TYPE)),
            "notificationTrigger" to listOf("unknownNotificationTrigger"),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = ParsingUtils.parseSubscription(payload, emptyList()).shouldSucceedAndResult()
        subscriptionService.validateNewSubscription(subscription)
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Unknown notification trigger in [unknownNotificationTrigger]"
            }
    }

    @Test
    fun `it should load a subscription with minimal required info - entities`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription = subscriptionService.getById(subscription.id)
        assertThat(persistedSubscription)
            .matches {
                it.id == "urn:ngsi-ld:Subscription:1".toUri() &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint.uri == URI("http://localhost:8084") &&
                    it.notification.endpoint.accept == Endpoint.AcceptType.JSON &&
                    (
                        it.entities != null &&
                            it.entities!!.size == 1 &&
                            it.entities!!.all { it.type == BEEHIVE_TYPE }
                        ) &&
                    it.watchedAttributes == null &&
                    it.isActive
            }
    }

    @Test
    fun `it should load a subscription with minimal required info - watchedAttributes`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_watched_attributes.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription = subscriptionService.getById(subscription.id)
        assertThat(persistedSubscription)
            .matches {
                it.id == "urn:ngsi-ld:Subscription:1".toUri() &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint.uri == URI("http://localhost:8084") &&
                    it.notification.endpoint.accept == Endpoint.AcceptType.JSON &&
                    it.entities == null &&
                    it.watchedAttributes == listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY) &&
                    it.isActive
            }
    }

    @Test
    fun `it should load a subscription with all possible members`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_full.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription = subscriptionService.getById(subscription.id)
        assertThat(persistedSubscription)
            .matches {
                it.id == "urn:ngsi-ld:Subscription:1".toUri() &&
                    it.subscriptionName == "A subscription with all possible members" &&
                    it.description == "A possible description" &&
                    (
                        it.entities != null &&
                            it.entities!!.size == 3 &&
                            it.entities!!.all { it.type == BEEHIVE_TYPE } &&
                            it.entities!!.any { it.id == "urn:ngsi-ld:Beehive:1234567890".toUri() } &&
                            it.entities!!.any { it.idPattern == "urn:ngsi-ld:Beehive:1234*" }
                        ) &&
                    it.watchedAttributes == listOf(INCOMING_PROPERTY) &&
                    it.notificationTrigger == listOf(
                        ENTITY_CREATED.notificationTrigger,
                        ATTRIBUTE_UPDATED.notificationTrigger,
                        ENTITY_DELETED.notificationTrigger
                    ) &&
                    it.timeInterval == null &&
                    it.q == "foodQuantity<150;foodName=='dietary fibres'" &&
                    (
                        it.geoQ != null &&
                            it.geoQ!!.georel == "within" &&
                            it.geoQ!!.geometry == "Polygon" &&
                            it.geoQ!!.coordinates ==
                            "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]" &&
                            it.geoQ!!.geoproperty == NGSILD_LOCATION_PROPERTY
                        ) &&
                    it.scopeQ == "/Nantes/+" &&
                    it.notification.attributes == listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY) &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint == Endpoint(
                        URI("http://localhost:8084"),
                        Endpoint.AcceptType.JSON,
                        listOf(EndpointInfo("Authorization-token", "Authorization-token-value"))
                    ) &&
                    it.expiresAt == ZonedDateTime.parse("2100-01-01T00:00:00Z")
            }
    }

    @Test
    fun `it should load a subscription with extra info on last notification`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(
                    mapOf("id" to "urn:ngsi-ld:smartDoor:77".toUri(), "type" to DEVICE_COMPACT_TYPE)
                )
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val notifiedAt = ngsiLdDateTime()
        subscriptionService.updateSubscriptionNotification(
            subscription,
            Notification(subscriptionId = subscription.id, notifiedAt = notifiedAt, data = emptyList()),
            true
        )

        val persistedSubscription = subscriptionService.getById(subscription.id)
        assertThat(persistedSubscription)
            .matches {
                it.notification.lastNotification != null &&
                    it.notification.lastNotification!!.isEqual(notifiedAt) &&
                    it.notification.lastSuccess != null &&
                    it.notification.lastSuccess!!.isEqual(notifiedAt) &&
                    it.notification.lastFailure == null &&
                    it.notification.timesSent == 1 &&
                    it.notification.status == StatusType.OK
            }
    }

    @Test
    fun `it should delete an existing subscription`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json")

        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        subscriptionService.delete(subscription.id).shouldSucceed()
    }

    @Test
    fun `it should not fail when trying to delete an unknown subscription`() = runTest {
        subscriptionService.delete("urn:ngsi-ld:Subscription:UnknownSubscription".toUri())
            .shouldSucceed()
    }

    @Test
    fun `it should not retrieve an expired subscription`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(mapOf("id" to "urn:ngsi-ld:Beekeeper:01", "type" to BEEKEEPER_COMPACT_TYPE)),
                "expiresAt" to ngsiLdDateTime().plusSeconds(1)
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        // add a delay to ensure subscription has expired
        runBlocking {
            delay(Duration.parse("2s"))
        }

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:01".toUri(),
                listOf(BEEKEEPER_TYPE),
                emptySet(),
                ATTRIBUTE_CREATED
            )

        assertThat(persistedSubscription).isEmpty()
    }

    @Test
    fun `it should retrieve a subscription whose expiration date has not been reached`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(mapOf("id" to "urn:ngsi-ld:Beekeeper:01", "type" to BEEKEEPER_COMPACT_TYPE)),
                "expiresAt" to ngsiLdDateTime().plusDays(1),
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:01".toUri(),
                listOf(BEEKEEPER_TYPE),
                emptySet(),
                ATTRIBUTE_CREATED
            )

        assertThat(persistedSubscription).hasSize(1)
    }

    @Test
    fun `it should retrieve a subscription matching an idPattern`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(
                    mapOf("idPattern" to "urn:ngsi-ld:Beekeeper:123*", "type" to BEEKEEPER_COMPACT_TYPE)
                )
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:12345678".toUri(),
                listOf(BEEKEEPER_TYPE),
                emptySet(),
                ATTRIBUTE_UPDATED
            )

        assertThat(persistedSubscription).hasSize(1)
    }

    @Test
    fun `it should not retrieve a subscription if idPattern does not match`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(
                    mapOf("idPattern" to "urn:ngsi-ld:Beekeeper:123*", "type" to BEEKEEPER_COMPACT_TYPE)
                )
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:3456789".toUri(),
                listOf(BEEKEEPER_TYPE),
                emptySet(),
                ATTRIBUTE_UPDATED
            )

        assertThat(persistedSubscription)
            .isEmpty()
    }

    @Test
    fun `it should retrieve a subscription matching a type`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(
                    mapOf("type" to BEEKEEPER_COMPACT_TYPE)
                )
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:01".toUri(),
                listOf(BEEKEEPER_TYPE),
                emptySet(),
                ATTRIBUTE_UPDATED
            )

        assertThat(persistedSubscription)
            .hasSize(1)
    }

    @Test
    fun `it should retrieve a subscription matching a type and an id`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(
                    mapOf("id" to "urn:ngsi-ld:Beekeeper:01", "type" to BEEKEEPER_COMPACT_TYPE)
                )
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:01".toUri(),
                listOf(BEEKEEPER_TYPE),
                emptySet(),
                ATTRIBUTE_UPDATED
            )

        assertThat(persistedSubscription)
            .hasSize(1)
    }

    @Test
    fun `it should not retrieve a subscription if type does not match`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(
                    mapOf("id" to "urn:ngsi-ld:Beekeeper:01", "type" to BEEKEEPER_COMPACT_TYPE)
                )
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:01".toUri(),
                listOf(SENSOR_TYPE),
                emptySet(),
                ATTRIBUTE_UPDATED
            )

        assertThat(persistedSubscription)
            .isEmpty()
    }

    @Test
    fun `it should not retrieve a deactivated subscription matching an id`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(
                    mapOf("id" to "urn:ngsi-ld:Beekeeper:01", "type" to BEEKEEPER_COMPACT_TYPE)
                ),
                "isActive" to false
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:01".toUri(),
                listOf(BEEKEEPER_TYPE),
                emptySet(),
                ATTRIBUTE_UPDATED
            )

        assertThat(persistedSubscription)
            .isEmpty()
    }

    @Test
    fun `it should retrieve a subscription matching one of the watched attributes`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "watchedAttributes" to listOf(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY)
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val subscriptions =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY),
                ATTRIBUTE_UPDATED
            )

        assertThat(subscriptions)
            .hasSize(1)
    }

    @Test
    fun `it should not retrieve a subscription not matching one of the watched attributes`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "watchedAttributes" to listOf(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY)
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val subscriptions =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(TEMPERATURE_PROPERTY),
                ATTRIBUTE_UPDATED
            )

        assertThat(subscriptions)
            .isEmpty()
    }

    @Test
    fun `it should not retrieve a subscription matching on type and not on one of the watched attributes`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(mapOf("type" to BEEHIVE_COMPACT_TYPE)),
                "watchedAttributes" to listOf(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY)
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val subscriptions =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(TEMPERATURE_PROPERTY),
                ATTRIBUTE_UPDATED
            )

        assertThat(subscriptions)
            .isEmpty()
    }

    @Test
    fun `it should retrieve a subscription without watched attributes matching on type`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf("entities" to listOf(mapOf("type" to BEEHIVE_COMPACT_TYPE)))
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val subscriptions =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(TEMPERATURE_PROPERTY),
                ATTRIBUTE_UPDATED
            )

        assertThat(subscriptions)
            .hasSize(1)
    }

    @Test
    fun `it should retrieve a subscription without watched attributes matching on type and id pattern`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf("entities" to listOf(mapOf("idPattern" to "urn:ngsi-ld:Beehive:*", "type" to BEEHIVE_COMPACT_TYPE)))
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val subscriptions =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(TEMPERATURE_PROPERTY),
                ATTRIBUTE_UPDATED
            )

        assertThat(subscriptions)
            .hasSize(1)
    }

    @Test
    fun `it should update a subscription`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val parsedInput = mapOf(
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "subscriptionName" to "My Subscription Updated",
            "description" to "My beautiful subscription has been updated",
            "q" to "foodQuantity>=150",
            "watchedAttributes" to arrayListOf(INCOMING_COMPACT_PROPERTY, TEMPERATURE_COMPACT_PROPERTY),
            "scopeQ" to "/A/#,/B",
            "geoQ" to mapOf(
                "georel" to "equals",
                "geometry" to "Point",
                "coordinates" to "[100.0, 0.0]",
                "geoproperty" to "https://uri.etsi.org/ngsi-ld/observationSpace"
            )
        )

        subscriptionService.update(subscription.id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))

        val updatedSubscription = subscriptionService.getById(subscription.id)
        assertThat(updatedSubscription)
            .matches {
                it.subscriptionName == "My Subscription Updated" &&
                    it.description == "My beautiful subscription has been updated" &&
                    it.q == "foodQuantity>=150" &&
                    it.watchedAttributes!! == listOf(INCOMING_PROPERTY, TEMPERATURE_PROPERTY) &&
                    it.scopeQ == "/A/#,/B" &&
                    it.geoQ!!.georel == "equals" &&
                    it.geoQ!!.geometry == "Point" &&
                    it.geoQ!!.coordinates == "[100.0, 0.0]" &&
                    it.geoQ!!.geoproperty == "https://uri.etsi.org/ngsi-ld/observationSpace"
            }
    }

    @Test
    fun `it should update a subscription notification`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val parsedInput = mapOf(
            "attributes" to listOf(OUTGOING_COMPACT_PROPERTY),
            "format" to "keyValues",
            "endpoint" to mapOf(
                "accept" to "application/ld+json",
                "uri" to "http://localhost:8080",
                "info" to listOf(
                    mapOf("key" to "Authorization-token", "value" to "Authorization-token-newValue")
                )
            )
        )

        subscriptionService.updateNotification(subscription.id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))

        val updatedSubscription = subscriptionService.getById(subscription.id)
        assertThat(updatedSubscription)
            .matches {
                it.notification.attributes == listOf(OUTGOING_PROPERTY) &&
                    it.notification.format.name == "KEY_VALUES" &&
                    it.notification.endpoint.accept.name == "JSONLD" &&
                    it.notification.endpoint.uri.toString() == "http://localhost:8080" &&
                    it.notification.endpoint.info == listOf(
                        EndpointInfo("Authorization-token", "Authorization-token-newValue")
                    ) &&
                    it.notification.endpoint.info!!.size == 1
            }
    }

    @Test
    fun `it should update a subscription entities`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val parsedInput = listOf(
            mapOf(
                "id" to "urn:ngsi-ld:Beehive:123",
                "type" to BEEHIVE_TYPE
            ),
            mapOf(
                "idPattern" to "urn:ngsi-ld:Beehive:12*",
                "type" to BEEHIVE_TYPE
            )
        )

        subscriptionService.updateEntities(subscription.id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))

        val updatedSubscription = subscriptionService.getById(subscription.id)
        assertThat(updatedSubscription)
            .matches {
                it.entities != null &&
                    it.entities!!.contains(
                        EntityInfo(
                            id = "urn:ngsi-ld:Beehive:123".toUri(),
                            idPattern = null,
                            type = BEEHIVE_TYPE
                        )
                    ) &&
                    it.entities!!.contains(
                        EntityInfo(
                            id = null,
                            idPattern = "urn:ngsi-ld:Beehive:12*",
                            type = BEEHIVE_TYPE
                        )
                    ) &&
                    it.entities!!.size == 2
            }
    }

    @Test
    fun `it should activate a subscription`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(mapOf("id" to "urn:ngsi-ld:Beekeeper:01", "type" to BEEKEEPER_COMPACT_TYPE)),
                "isActive" to false
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        subscriptionService.update(
            subscription.id,
            mapOf("type" to NGSILD_SUBSCRIPTION_TERM, "isActive" to true),
            listOf(APIC_COMPOUND_CONTEXT)
        ).shouldSucceed()

        val updatedSubscription = subscriptionService.getById(subscription.id)
        assertThat(updatedSubscription)
            .matches {
                it.isActive && it.modifiedAt != null
            }
    }

    @Test
    fun `it should deactivate a subscription`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(mapOf("id" to "urn:ngsi-ld:Beekeeper:01", "type" to BEEKEEPER_COMPACT_TYPE))
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        subscriptionService.update(
            subscription.id,
            mapOf("type" to NGSILD_SUBSCRIPTION_TERM, "isActive" to false),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        val updatedSubscription = subscriptionService.getById(subscription.id)
        assertThat(updatedSubscription)
            .matches {
                !it.isActive && it.modifiedAt != null
            }
    }

    @Test
    fun `it should update and expand watched attributes of a subscription`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "watchedAttributes" to arrayListOf(INCOMING_COMPACT_PROPERTY)
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val parsedInput = mapOf(
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "watchedAttributes" to arrayListOf(INCOMING_COMPACT_PROPERTY, TEMPERATURE_COMPACT_PROPERTY)
        )

        subscriptionService.update(subscription.id, parsedInput, listOf(APIC_COMPOUND_CONTEXT)).shouldSucceed()

        val updatedSubscription = subscriptionService.getById(subscription.id)

        assertThat(updatedSubscription)
            .matches {
                it.watchedAttributes!! == listOf(INCOMING_PROPERTY, TEMPERATURE_PROPERTY) &&
                    it.modifiedAt != null
            }
    }

    @Test
    fun `it should update notification trigger of a subscription`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(mapOf("id" to "urn:ngsi-ld:Beekeeper:01", "type" to BEEKEEPER_COMPACT_TYPE)),
                "notificationTrigger" to listOf(ENTITY_CREATED.notificationTrigger)
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val notificationTriggers = arrayListOf(
            ENTITY_CREATED.notificationTrigger,
            ENTITY_DELETED.notificationTrigger
        )
        val parsedInput = mapOf(
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "notificationTrigger" to notificationTriggers
        )
        subscriptionService.update(subscription.id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))

        val updatedSubscription = subscriptionService.getById(subscription.id)
        assertThat(updatedSubscription)
            .matches {
                it.notificationTrigger == notificationTriggers &&
                    it.modifiedAt != null
            }
    }

    @Test
    fun `it should return a BadRequestData exception if the subscription has an unknown attribute`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val parsedInput = mapOf("type" to NGSILD_SUBSCRIPTION_TERM, "unknownAttribute" to "unknownValue")

        subscriptionService.update(subscription.id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Subscription urn:ngsi-ld:Subscription:1 has invalid attribute: unknownAttribute"
            }
    }

    @Test
    fun `it should return a NotImplemented exception if the subscription has an unsupported attribute`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val parsedInput = mapOf("type" to NGSILD_SUBSCRIPTION_TERM, "throttling" to "someValue")

        subscriptionService.update(subscription.id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
            .shouldFailWith {
                it is NotImplementedException &&
                    it.message == "Subscription urn:ngsi-ld:Subscription:1 has unsupported attribute: throttling"
            }
    }

    @Test
    fun `it should return true if query is null`() = runTest {
        val query = null
        subscriptionService.isMatchingQQuery(query, jsonldEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should return false if query is invalid`() = runTest {
        val query = "foodQuantity.invalidAttribute>=150"
        subscriptionService.isMatchingQQuery(query, jsonldEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should return true if entity matches query`() = runTest {
        val query = "foodQuantity<150"
        subscriptionService.isMatchingQQuery(query, jsonldEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should return false if entity doesn't match query`() = runTest {
        val query = "foodQuantity>=150"
        subscriptionService.isMatchingQQuery(query, jsonldEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should support multiple predicates query`() = runTest {
        val query = "(foodQuantity<=150;foodName==\"dietary fibres\");executes==urn:ngsi-ld:Feeder:018z5"
        subscriptionService.isMatchingQQuery(query, jsonldEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should support multiple predicates query with logical operator`() = runTest {
        val query = "foodQuantity>150;executes.createdAt==\"2018-11-26T21:32:52.98601Z\""
        subscriptionService.isMatchingQQuery(query, jsonldEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should support multiple predicates query with or logical operator`() = runTest {
        val query = "foodQuantity>150|executes.createdAt==\"2018-11-26T21:32:52.98601Z\""
        subscriptionService.isMatchingQQuery(query, jsonldEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should support boolean value type`() = runTest {
        val query = "foodName.isHealthy!=false"
        subscriptionService.isMatchingQQuery(query, jsonldEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should return true if entity matches scope query`() = runTest {
        val scopeQuery = "/Nantes/#"
        subscriptionService.isMatchingScopeQQuery(scopeQuery, jsonldEntity)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should return false if entity does not match scope query`() = runTest {
        val scopeQuery = "/Valbonne/#"
        subscriptionService.isMatchingScopeQQuery(scopeQuery, jsonldEntity)
            .shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it shoud return true if a subscription has no geoquery`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        subscriptionService.isMatchingGeoQuery(subscription.id, jsonldEntity)
            .shouldSucceedWith { assertTrue(it) }
    }

    @ParameterizedTest
    @CsvSource(
        "near;minDistance==1000, Polygon, '[[[100.0, 0.0], [101.0, 0.0], [101.0, -1.0], [100.0, 0.0]]]', false",
        "near;maxDistance==1000, Polygon, '[[[100.0, 0.0], [101.0, 0.0], [101.0, -1.0], [100.0, 0.0]]]', true",
        "contains, Polygon, '[[[90.0, 0.0], [100.0, 10.0], [110.0, 0.0], [100.0, -10.0], [90.0, 0.0]]]', true",
        "contains, Polygon, '[[[80.0, 0.0], [90.0, 5.0], [90.0, 0.0], [80.0, 0.0]]]', false",
        "equals, Point, '[100.0, 0.0]', true",
        "equals, Point, '[101.0, 0.0]', false",
        "intersects, Polygon, '[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]', true",
        "intersects, Polygon, '[[[101.0, 0.0], [102.0, 0.0], [102.0, -1.0], [101.0, 0.0]]]', false",
        "disjoint, Point, '[101.0, 0.0]', true",
        "disjoint, Polygon, '[[[100.0, 0.0], [101.0, 0.0], [101.0, -1.0], [100.0, 0.0]]]', false"
    )
    fun `it shoud correctly matches the geoquery provided with a subscription`(
        georel: String,
        geometry: String,
        coordinates: String,
        expectedResult: Boolean
    ) = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "watchedAttributes" to listOf(INCOMING_COMPACT_PROPERTY),
                "geoQ" to mapOf(
                    "georel" to georel,
                    "geometry" to geometry,
                    "coordinates" to coordinates
                )
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        subscriptionService.isMatchingGeoQuery(subscription.id, jsonldEntity)
            .shouldSucceedWith { assertEquals(expectedResult, it) }
    }

    @Test
    fun `it should return all subscriptions whose 'timeInterval' is reached `() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(mapOf("type" to BEEKEEPER_COMPACT_TYPE)),
                "timeInterval" to 500
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val subscription2 = gimmeSubscriptionFromMembers(
            mapOf(
                "id" to "urn:ngsi-ld:Subscription:02".toUri(),
                "entities" to listOf(mapOf("type" to BEEKEEPER_COMPACT_TYPE)),
                "timeInterval" to 5000
            )
        )
        subscriptionService.create(subscription2, mockUserSub).shouldSucceed()

        val persistedSubscription = subscriptionService.getById(subscription.id)
        val notification = Notification(subscriptionId = subscription.id, data = emptyList())

        subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)

        val subscriptionsToNotify = subscriptionService.getRecurringSubscriptionsToNotify()
        assertEquals(1, subscriptionsToNotify.size)

        subscriptionService.delete(subscription.id)
        subscriptionService.delete(subscription2.id)
    }

    @Test
    fun `it should return all subscriptions whose 'timeInterval' is reached with a time of 5s`() = runTest {
        val subscription = gimmeSubscriptionFromMembers(
            mapOf(
                "entities" to listOf(mapOf("type" to BEEKEEPER_COMPACT_TYPE)),
                "timeInterval" to 1
            )
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        val subscription2 = gimmeSubscriptionFromMembers(
            mapOf(
                "id" to "urn:ngsi-ld:Subscription:02".toUri(),
                "entities" to listOf(mapOf("type" to BEEKEEPER_COMPACT_TYPE)),
                "timeInterval" to 5000
            )
        )
        subscriptionService.create(subscription2, mockUserSub).shouldSucceed()

        val persistedSubscription = subscriptionService.getById(subscription.id)
        val notification = Notification(subscriptionId = subscription.id, data = emptyList())

        val persistedSubscription2 = subscriptionService.getById(subscription2.id)
        val notification2 = Notification(subscriptionId = subscription2.id, data = emptyList())

        subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)

        subscriptionService.updateSubscriptionNotification(persistedSubscription2, notification2, true)

        runBlocking {
            delay(5000)
        }
        val subscriptionsToNotify = subscriptionService.getRecurringSubscriptionsToNotify()
        assertEquals(1, subscriptionsToNotify.size)

        subscriptionService.delete(subscription.id)
        subscriptionService.delete(subscription2.id)
    }

    @Test
    fun `it should retrieve the JSON-LD contexts of subscription`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription.jsonld")
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        subscriptionService.getContextsForSubscription(subscription.id)
            .shouldSucceedWith {
                assertEquals(listOf(APIC_COMPOUND_CONTEXT), it)
            }
    }

    @Test
    fun `it should return a link to contexts endpoint if subscription has more than one context`() = runTest {
        val subscription = loadAndDeserializeSubscription("subscription_minimal_entities.json").copy(
            contexts = listOf(APIC_COMPOUND_CONTEXT, NGSILD_CORE_CONTEXT)
        )

        val contextLink = subscriptionService.getContextsLink(subscription)

        assertThat(contextLink)
            .contains("/ngsi-ld/v1/subscriptions/${subscription.id}/context")
    }
}
