package com.egm.stellio.subscription.service

import arrow.core.Some
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import com.egm.stellio.subscription.model.NotificationParams.StatusType
import com.egm.stellio.subscription.support.WithTimescaleContainer
import com.egm.stellio.subscription.utils.ParsingUtils
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
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

    private val mockUserSub = Some(UUID.randomUUID().toString())

    private lateinit var subscription1Id: URI
    private lateinit var subscription2Id: URI
    private lateinit var subscription3Id: URI
    private lateinit var subscription4Id: URI
    private lateinit var subscription5Id: URI
    private lateinit var subscription6Id: URI
    private lateinit var subscription7Id: URI

    private val entity =
        ClassPathResource("/ngsild/aquac/FeedingService.json").inputStream.readBytes().toString(Charsets.UTF_8)

    private lateinit var jsonldEntity: JsonLdEntity

    @BeforeAll
    fun bootstrapSubscriptions() {
        runBlocking {
            jsonldEntity = JsonLdUtils.expandJsonLdEntity(entity, listOf(APIC_COMPOUND_CONTEXT))
        }

        createSubscription1()
        createSubscription2()
        createSubscription3()
        createSubscription4()
        createSubscription5()
        createSubscription6()
        createSubscription7()
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
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY))
        ).copy(
            subscriptionName = "Subscription 1",
            scopeQ = "/A/+/C,/B",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEHIVE_TYPE),
                EntityInfo(id = null, idPattern = "urn:ngsi-ld:Beekeeper:1234*", type = BEEKEEPER_TYPE)
            )
        )
        subscription1Id = createSubscription(subscription)
    }

    private fun createSubscription2() {
        val subscription = gimmeRawSubscription(
            withEndpointInfo = true,
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY))
        ).copy(
            subscriptionName = "Subscription 2",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEKEEPER_TYPE),
                EntityInfo(id = "urn:ngsi-ld:Beehive:1234567890".toUri(), idPattern = null, type = BEEHIVE_TYPE)
            ),
            expiresAt = Instant.now().atZone(ZoneOffset.UTC).plusDays(1)
        )
        subscription2Id = createSubscription(subscription)
    }

    private fun createSubscription3() {
        val subscription = gimmeRawSubscription(
            withEndpointInfo = false,
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY))
        ).copy(
            subscriptionName = "Subscription 3",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = APIARY_TYPE)
            ),
            isActive = false
        )
        subscription3Id = createSubscription(subscription)
    }

    private fun createSubscription4() {
        val subscription = gimmeRawSubscription(
            withEndpointInfo = false,
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY))
        ).copy(
            subscriptionName = "Subscription 4",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEHIVE_TYPE)
            ),
            isActive = false,
            watchedAttributes = listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
        )
        subscription4Id = createSubscription(subscription)
    }

    private fun createSubscription5() {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY))
        ).copy(
            subscriptionName = "Subscription 5",
            entities = setOf(
                EntityInfo(id = "urn:ngsi-ld:smartDoor:77".toUri(), idPattern = null, type = DEVICE_TYPE)
            ),
            isActive = true,
            expiresAt = null
        )
        subscription5Id = createSubscription(subscription)
    }

    private fun createSubscription6() {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY))
        ).copy(
            subscriptionName = "Subscription 6",
            entities = setOf(
                EntityInfo(id = "urn:ngsi-ld:smartDoor:88".toUri(), idPattern = null, type = DEVICE_TYPE)
            ),
            isActive = false,
            expiresAt = ZonedDateTime.parse("2012-08-12T08:33:38Z")
        )
        subscription6Id = createSubscription(subscription)
    }

    private fun createSubscription7() {
        val subscription = gimmeRawSubscription().copy(
            subscriptionName = "Subscription 7",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = APIARY_TYPE)
            ),
            contexts = listOf(APIC_COMPOUND_CONTEXT)
        )
        subscription7Id = createSubscription(subscription)
    }

    @Test
    fun `it should not retrieve an expired subscription matching an id`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:smartDoor:88".toUri(),
                listOf(DEVICE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription).isEmpty()
    }

    @Test
    fun `it should retrieve a subscription matching an id when expired date is not given`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:smartDoor:77".toUri(),
                listOf(DEVICE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription).hasSize(1)
    }

    @Test
    fun `it should retrieve a subscription matching an id when expired date is in the future`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription).hasSize(2)
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
            .shouldFail {
                it is BadRequestDataException &&
                    it.message == "Invalid value for idPattern: ["
            }
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info`() = runTest {
        val persistedSubscription = subscriptionService.getById(subscription1Id)

        assertThat(persistedSubscription)
            .matches {
                it.subscriptionName == "Subscription 1" &&
                    it.description == "My beautiful subscription" &&
                    it.notification.attributes == listOf(INCOMING_PROPERTY) &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint ==
                    Endpoint(
                        URI("http://localhost:8089/notification"),
                        Endpoint.AcceptType.JSONLD,
                        null
                    ) &&
                    it.entities.size == 2 &&
                    it.entities.any { entityInfo ->
                        entityInfo.type == BEEKEEPER_TYPE &&
                            entityInfo.id == null &&
                            entityInfo.idPattern == "urn:ngsi-ld:Beekeeper:1234*"
                    } &&
                    it.entities.any { entityInfo ->
                        entityInfo.type == BEEHIVE_TYPE &&
                            entityInfo.id == null &&
                            entityInfo.idPattern == null
                    } &&
                    it.geoQ == null
            }
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and query`() = runTest {
        val persistedSubscription = subscriptionService.getById(subscription3Id)

        assertThat(persistedSubscription)
            .matches {
                it.subscriptionName == "Subscription 3" &&
                    it.description == "My beautiful subscription" &&
                    it.q == "speed>50;foodName==dietary fibres" &&
                    it.notification.attributes == listOf(INCOMING_PROPERTY) &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint == Endpoint(
                        URI("http://localhost:8089/notification"),
                        Endpoint.AcceptType.JSONLD,
                        null
                    ) &&
                    it.entities.size == 1
            }
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and geoquery and endpoint info`() =
        runTest {
            val persistedSubscription = subscriptionService.getById(subscription2Id)

            assertThat(persistedSubscription)
                .matches {
                    it.subscriptionName == "Subscription 2" &&
                        it.description == "My beautiful subscription" &&
                        it.notification.attributes == listOf(INCOMING_PROPERTY) &&
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
                        it.geoQ!!.coordinates ==
                        "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]"
                }
        }

    @Test
    fun `it should load and fill a persisted subscription with entities info and active status`() = runTest {
        val persistedSubscription = subscriptionService.getById(subscription2Id)

        assertThat(persistedSubscription)
            .matches {
                it.subscriptionName == "Subscription 2" &&
                    it.description == "My beautiful subscription" &&
                    it.notification.attributes == listOf(INCOMING_PROPERTY) &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint == Endpoint(
                        URI("http://localhost:8089/notification"),
                        Endpoint.AcceptType.JSONLD,
                        listOf(EndpointInfo("Authorization-token", "Authorization-token-value"))
                    ) &&
                    it.entities.size == 2 &&
                    it.isActive
            }
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and inactive status`() = runTest {
        val persistedSubscription = subscriptionService.getById(subscription4Id)

        assertThat(persistedSubscription)
            .matches {
                it.subscriptionName == "Subscription 4" &&
                    it.description == "My beautiful subscription" &&
                    it.notification.attributes == listOf(INCOMING_PROPERTY) &&
                    it.notification.format == FormatType.NORMALIZED &&
                    it.notification.endpoint == Endpoint(
                        URI("http://localhost:8089/notification"),
                        Endpoint.AcceptType.JSONLD,
                        null
                    ) &&
                    it.entities.size == 1 &&
                    !it.isActive
            }
    }

    @Test
    fun `it should load and fill a persisted subscription with entities info and null value for watched attributes`() =
        runTest {
            val persistedSubscription = subscriptionService.getById(subscription2Id)

            assertThat(persistedSubscription)
                .matches {
                    it.subscriptionName == "Subscription 2" &&
                        it.description == "My beautiful subscription" &&
                        it.entities.size == 2 &&
                        it.watchedAttributes == null
                }
        }

    @Test
    fun `it should load and fill a persisted subscription with the correct format for temporal values`() = runTest {
        val createdAt = Instant.now().truncatedTo(ChronoUnit.MICROS).atZone(ZoneOffset.UTC)
        val subscription = gimmeRawSubscription().copy(
            createdAt = createdAt,
            entities = setOf(
                EntityInfo(id = "urn:ngsi-ld:smartDoor:77".toUri(), idPattern = null, type = DEVICE_TYPE)
            )
        )
        val notifiedAt = ngsiLdDateTime()

        subscriptionService.create(subscription, mockUserSub)
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
                    it.createdAt.isEqual(createdAt)
            }

        subscriptionService.delete(subscription.id)
    }

    @Test
    fun `it should delete an existing subscription`() = runTest {
        val subscription = gimmeRawSubscription()

        subscriptionService.create(subscription, mockUserSub)

        subscriptionService.delete(subscription.id)
            .shouldSucceed()
    }

    @Test
    fun `it should not fail when trying to delete an unknown subscription`() = runTest {
        subscriptionService.delete("urn:ngsi-ld:Subscription:UnknownSubscription".toUri())
            .shouldSucceed()
    }

    @Test
    fun `it should retrieve a subscription matching an idPattern`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:12345678".toUri(),
                listOf(BEEKEEPER_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription).hasSize(2)
    }

    @Test
    fun `it should not retrieve a subscription if idPattern does not match`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beekeeper:9876543".toUri(),
                listOf(BEEKEEPER_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription)
            .hasSize(1)
            .element(0).matches {
                it.subscriptionName == "Subscription 2"
            }
    }

    @Test
    fun `it should retrieve a subscription matching a type and not one with non matching id`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:ABCD".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription)
            .hasSize(1)
            .element(0).matches {
                it.subscriptionName == "Subscription 1" &&
                    it.notification.endpoint == Endpoint(
                        URI("http://localhost:8089/notification"),
                        Endpoint.AcceptType.JSONLD
                    ) &&
                    it.entities.isEmpty()
            }
    }

    @Test
    fun `it should retrieve a subscription matching a type and an exact id`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription)
            .hasSize(2)
    }

    @Test
    fun `it should retrieve a subscription matching an id`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription)
            .hasSize(1)
    }

    @Test
    fun `it should not retrieve a subscription if type does not match`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Sensor:1234567890".toUri(),
                listOf(SENSOR_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription)
            .isEmpty()
    }

    @Test
    fun `it should retrieve an activated subscription matching an id`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:smartDoor:77".toUri(),
                listOf(DEVICE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription)
            .hasSize(1)
            .element(0).matches {
                it.subscriptionName == "Subscription 5"
            }
    }

    @Test
    fun `it should not retrieve a deactivated subscription matching an id`() = runTest {
        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:smartDoor:88".toUri(),
                listOf(DEVICE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription)
            .isEmpty()
    }

    @Test
    fun `it should retrieve a subscription if watched attributes is null`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            subscriptionName = "My subscription",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEHIVE_TYPE)
            ),
            watchedAttributes = null
        )

        subscriptionService.create(subscription, mockUserSub)

        val persistedSubscription =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(persistedSubscription)
            .anyMatch { it.id == subscription.id }

        subscriptionService.delete(subscription.id)
    }

    @Test
    fun `it should retrieve a subscription if watched attributes contains at least one of the updated attributes`() =
        runTest {
            val subscription = gimmeRawSubscription().copy(
                subscriptionName = "My subscription",
                entities = setOf(
                    EntityInfo(id = null, idPattern = null, type = BEEHIVE_TYPE)
                ),
                watchedAttributes = listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY, TEMPERATURE_PROPERTY)
            )

            subscriptionService.create(subscription, mockUserSub)

            val subscriptions =
                subscriptionService.getMatchingSubscriptions(
                    "urn:ngsi-ld:Beehive:1234567890".toUri(),
                    listOf(BEEHIVE_TYPE),
                    setOf(INCOMING_PROPERTY)
                )

            assertThat(subscriptions)
                .anyMatch { it.id == subscription.id }

            subscriptionService.delete(subscription.id)
        }

    @Test
    fun `it should not retrieve a subscription if watched attributes do not match any updated attributes`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            subscriptionName = "My subscription",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEHIVE_TYPE)
            ),
            watchedAttributes = listOf(OUTGOING_PROPERTY, TEMPERATURE_PROPERTY)
        )

        subscriptionService.create(subscription, mockUserSub)

        val subscriptions =
            subscriptionService.getMatchingSubscriptions(
                "urn:ngsi-ld:Beehive:1234567890".toUri(),
                listOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY)
            )

        assertThat(subscriptions)
            .allMatch { it.id != subscription.id }

        subscriptionService.delete(subscription.id)
    }

    @Test
    fun `it should update a subscription`() = runTest {
        val parsedInput = mapOf(
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "subscriptionName" to "My Subscription Updated",
            "description" to "My beautiful subscription has been updated",
            "q" to "foodQuantity>=150",
            "scopeQ" to "/A/#,/B",
            "geoQ" to mapOf(
                "georel" to "equals",
                "geometry" to "Point",
                "coordinates" to "[100.0, 0.0]",
                "geoproperty" to "https://uri.etsi.org/ngsi-ld/observationSpace"
            )
        )

        subscriptionService.update(subscription4Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
        val subscription = subscriptionService.getById(subscription4Id)

        assertThat(subscription)
            .matches {
                it.subscriptionName == "My Subscription Updated" &&
                    it.description == "My beautiful subscription has been updated" &&
                    it.q == "foodQuantity>=150" &&
                    it.scopeQ == "/A/#,/B" &&
                    it.geoQ!!.georel == "equals" &&
                    it.geoQ!!.geometry == "Point" &&
                    it.geoQ!!.coordinates == "[100.0, 0.0]" &&
                    it.geoQ!!.geoproperty == "https://uri.etsi.org/ngsi-ld/observationSpace"
            }
    }

    @Test
    fun `it should update a subscription notification`() = runTest {
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

        subscriptionService.updateNotification(subscription4Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
        val subscription = subscriptionService.getById(subscription4Id)

        assertThat(subscription)
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

        subscriptionService.updateEntities(subscription4Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))
        val subscription = subscriptionService.getById(subscription4Id)

        assertThat(subscription)
            .matches {
                it.entities.contains(
                    EntityInfo(
                        id = "urn:ngsi-ld:Beehive:123".toUri(),
                        idPattern = null,
                        type = BEEHIVE_TYPE
                    )
                ) &&
                    it.entities.contains(
                        EntityInfo(
                            id = null,
                            idPattern = "urn:ngsi-ld:Beehive:12*",
                            type = BEEHIVE_TYPE
                        )
                    ) &&
                    it.entities.size == 2
            }
    }

    @Test
    fun `it should activate a subscription`() = runTest {
        subscriptionService.update(
            subscription3Id,
            mapOf("type" to NGSILD_SUBSCRIPTION_TERM, "isActive" to true),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        val subscription = subscriptionService.getById(subscription3Id)

        assertThat(subscription)
            .matches {
                it.isActive && it.modifiedAt != null
            }
    }

    @Test
    fun `it should deactivate a subscription`() = runTest {
        subscriptionService.update(
            subscription1Id,
            mapOf("type" to NGSILD_SUBSCRIPTION_TERM, "isActive" to false),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        val subscription = subscriptionService.getById(subscription1Id)

        assertThat(subscription)
            .matches {
                !it.isActive && it.modifiedAt != null
            }
    }

    @Test
    fun `it should update a subscription watched attributes`() = runTest {
        val parsedInput = mapOf(
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "watchedAttributes" to arrayListOf(INCOMING_PROPERTY, TEMPERATURE_PROPERTY)
        )

        subscriptionService.update(subscription5Id, parsedInput, listOf(APIC_COMPOUND_CONTEXT))

        val subscription = subscriptionService.getById(subscription5Id)

        assertThat(subscription)
            .matches {
                it.watchedAttributes!! == listOf(INCOMING_PROPERTY, TEMPERATURE_PROPERTY) &&
                    it.modifiedAt != null
            }
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
    fun `it should update a subscription with a notification result`() = runTest {
        val persistedSubscription = subscriptionService.getById(subscription1Id)
        val notification = Notification(subscriptionId = subscription1Id, data = emptyList())

        subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)

        val subscription = subscriptionService.getById(subscription1Id)
        assertThat(subscription)
            .matches {
                it.id == subscription1Id &&
                    it.notification.status == StatusType.OK &&
                    it.notification.timesSent == 1 &&
                    it.notification.lastNotification != null &&
                    it.notification.lastSuccess != null &&
                    it.notification.lastFailure == null
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
        subscriptionService.isMatchingGeoQuery(subscription1Id, jsonldEntity)
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
        val subscription = gimmeRawSubscription(
            withQueryAndGeoQuery = Pair(false, true),
            georel = georel,
            geometry = geometry,
            coordinates = coordinates
        )
        subscriptionService.create(subscription, mockUserSub).shouldSucceed()

        subscriptionService.isMatchingGeoQuery(subscription.id, jsonldEntity)
            .shouldSucceedWith { assertEquals(expectedResult, it) }
    }

    @Test
    fun `it should return all subscriptions whose 'timeInterval' is reached `() = runTest {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY)),
            timeInterval = 500
        ).copy(
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEHIVE_TYPE)
            )
        )

        val subscriptionId = createSubscription(subscription)

        val subscription2 = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY)),
            timeInterval = 5000
        ).copy(
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEKEEPER_TYPE)
            )
        )
        val subscriptionId2 = createSubscription(subscription2)

        val persistedSubscription = subscriptionService.getById(subscriptionId)
        val notification = Notification(subscriptionId = subscriptionId, data = emptyList())

        subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)

        val subscriptionsToNotify = subscriptionService.getRecurringSubscriptionsToNotify()
        assertEquals(1, subscriptionsToNotify.size)

        subscriptionService.delete(subscriptionId)
        subscriptionService.delete(subscriptionId2)
    }

    @Test
    fun `it should return all subscriptions whose 'timeInterval' is reached with a time of 5s`() = runTest {
        val subscription = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY)),
            timeInterval = 1
        ).copy(
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEHIVE_TYPE)
            )
        )

        val subscriptionId1 = createSubscription(subscription)

        val subscription2 = gimmeRawSubscription(
            withNotifParams = Pair(FormatType.NORMALIZED, listOf(INCOMING_PROPERTY)),
            timeInterval = 5000
        ).copy(
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = BEEKEEPER_TYPE)
            )
        )
        val subscriptionId2 = createSubscription(subscription2)

        val persistedSubscription = subscriptionService.getById(subscriptionId1)
        val notification = Notification(subscriptionId = subscriptionId1, data = emptyList())

        val persistedSubscription2 = subscriptionService.getById(subscriptionId2)
        val notification2 = Notification(subscriptionId = subscriptionId2, data = emptyList())

        subscriptionService.updateSubscriptionNotification(persistedSubscription, notification, true)

        subscriptionService.updateSubscriptionNotification(persistedSubscription2, notification2, true)

        runBlocking {
            delay(5000)
        }
        val subscriptionsToNotify = subscriptionService.getRecurringSubscriptionsToNotify()
        assertEquals(1, subscriptionsToNotify.size)

        subscriptionService.delete(subscriptionId1)
        subscriptionService.delete(subscriptionId2)
    }

    @Test
    fun `it should retrieve a context of subscription`() = runTest {
        subscriptionService.getContextsForSubscription(subscription7Id)
            .shouldSucceedWith {
                assertEquals(listOf(APIC_COMPOUND_CONTEXT), it)
            }
    }

    @Test
    fun `it should return a link to contexts endpoint if subscription has more than one context`() = runTest {
        val subscription = gimmeRawSubscription().copy(
            contexts = listOf(APIC_COMPOUND_CONTEXT, NGSILD_CORE_CONTEXT)
        )

        val contextLink = subscriptionService.getContextsLink(subscription)

        assertThat(contextLink)
            .contains("/ngsi-ld/v1/subscriptions/${subscription.id}/context")
    }
}
