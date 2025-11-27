package com.egm.stellio.subscription.model

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_OPERATION_SPACE_IRI
import com.egm.stellio.shared.model.NGSILD_OPERATION_SPACE_TERM
import com.egm.stellio.shared.model.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.ApiUtils.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.BEEKEEPER_IRI
import com.egm.stellio.shared.util.BEEKEEPER_TERM
import com.egm.stellio.shared.util.DateUtils.ngsiLdDateTime
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.deserializeListOfObjects
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.OUTGOING_TERM
import com.egm.stellio.shared.util.UriUtils.toUri
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.subscription.model.Subscription.Companion.deserialize
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.springframework.http.MediaType

class SubscriptionTests {

    private val endpointReceiverInfo =
        listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value"))

    private val beehiveId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    private val subscription = Subscription(
        id = "urn:ngsi-ld:Subscription:01".toUri(),
        type = NGSILD_SUBSCRIPTION_TERM,
        entities = setOf(
            EntitySelector(
                id = null,
                idPattern = null,
                typeSelection = BEEHIVE_IRI
            )
        ),
        geoQ = GeoQ(
            georel = "within",
            geometry = "Polygon",
            coordinates = "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]",
            pgisGeometry = "100000101000101010100010100054300",
            geoproperty = NGSILD_OPERATION_SPACE_IRI
        ),
        watchedAttributes = listOf(INCOMING_IRI, OUTGOING_IRI),
        notification = NotificationParams(
            attributes = listOf(INCOMING_IRI),
            format = NotificationParams.FormatType.KEY_VALUES,
            endpoint = Endpoint(
                uri = "http://localhost:8089/notification".toUri(),
                accept = Endpoint.AcceptType.JSONLD,
                receiverInfo = endpointReceiverInfo
            )
        ),
        contexts = APIC_COMPOUND_CONTEXTS
    )

    @Test
    fun `it should correctly deserialize a subscription`() = runTest {
        val subscription = mapOf(
            "id" to beehiveId,
            "type" to "Subscription",
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        deserialize(subscription, emptyList()).shouldSucceedWith {
            assertNotNull(it)
        }
    }

    @Test
    fun `it should not validate a subscription with an empty id`() = runTest {
        val payload = mapOf(
            "id" to "",
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "The supplied identifier was expected to be an URI but it is not: "
            }
    }

    @Test
    fun `it should not validate a subscription with an invalid id`() = runTest {
        val payload = mapOf(
            "id" to "invalidId",
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "The supplied identifier was expected to be an URI but it is not: invalidId"
            }
    }

    @Test
    fun `it should not validate a subscription with an invalid idPattern`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI, "idPattern" to "[")),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Invalid idPattern found in subscription"
            }
    }

    @Test
    fun `it should not validate a subscription without entities and watchedAttributes`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "At least one of entities or watchedAttributes shall be present"
            }
    }

    @Test
    fun `it should not validate a subscription with timeInterval and watchedAttributes`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "timeInterval" to 10,
            "watchedAttributes" to listOf(INCOMING_TERM),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "You can't use 'timeInterval' in conjunction with 'watchedAttributes'"
            }
    }

    @Test
    fun `it should not validate a subscription with timeInterval and throttling`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "timeInterval" to 10,
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy")),
            "throttling" to 30
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "You can't use 'timeInterval' in conjunction with 'throttling'"
            }
    }

    @Test
    fun `it should not validate a subscription with a negative throttling`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy")),
            "throttling" to -30
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "The value of 'throttling' must be greater than zero (int)"
            }
    }

    @Test
    fun `it should not validate a subscription with a negative timeInterval`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "watchedAttributes" to listOf(INCOMING_TERM),
            "timeInterval" to -10,
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "The value of 'timeInterval' must be greater than zero (int)"
            }
    }

    @Test
    fun `it should not validate a subscription with an expiresAt in the past`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "watchedAttributes" to listOf(INCOMING_TERM),
            "expiresAt" to ngsiLdDateTime().minusDays(1),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "'expiresAt' must be in the future"
            }
    }

    @Test
    fun `it should not validate a subscription with an unknown notification trigger`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI)),
            "notificationTrigger" to listOf("unknownNotificationTrigger"),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Unknown notification trigger in [unknownNotificationTrigger]"
            }
    }

    @Test
    fun `it should not validate a subscription with an invalid endpoint URI`() = runTest {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Beehive:1234567890".toUri(),
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "endoint_is_not_a_valid_uri"))
        )

        val subscription = deserialize(payload, emptyList()).shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Invalid URI for endpoint: endoint_is_not_a_valid_uri"
            }
    }

    @Test
    fun `it should not allow a subscription if remote JSON-LD @context cannot be retrieved`() = runTest {
        val contextNonExisting = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-non-existing.jsonld"
        val subscription = mapOf(
            "id" to "urn:ngsi-ld:BeeHive:01",
            "type" to NGSILD_SUBSCRIPTION_TERM,
            "entities" to listOf(mapOf("type" to BEEHIVE_IRI)),
            "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
        )

        val result = deserialize(subscription, listOf(contextNonExisting))
        result.fold({
            assertTrue(it is LdContextNotAvailableException)
            assertEquals(
                "Unable to load remote context",
                it.message
            )
            assertEquals(
                "caused by: JsonLdError[code=There was a problem encountered " +
                    "loading a remote context [code=LOADING_REMOTE_CONTEXT_FAILED]., message=There was a problem " +
                    "encountered loading a remote context " +
                    "[https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-non-existing.jsonld]]",
                it.detail
            )
        }, {
            fail("it should not have allowed a subscription if remote JSON-LD @context cannot be retrieved")
        })
    }

    @Test
    fun `it should not validate a subscription when jsonldContext is not a URI`() = runTest {
        val rawSubscription =
            """
                {
                    "id": "urn:ngsi-ld:Subscription:1234567890",
                    "type": "Subscription",
                    "entities": [
                      {
                        "type": "BeeHive"
                      }
                    ],
                    "notification": {
                       "endpoint": {
                         "uri": "http://localhost:8084"
                       }
                    },
                    "jsonldContext": "unknownContext"
                }
            """.trimIndent()

        val subscription = deserialize(
            rawSubscription.deserializeAsMap(),
            emptyList()
        ).shouldSucceedAndResult()
        subscription.validate()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
            }
    }

    @Test
    fun `it should not validate a subscription when jsonldContext is not available`() = runTest {
        val rawSubscription =
            """
                {
                    "id": "urn:ngsi-ld:Subscription:1234567890",
                    "type": "Subscription",
                    "entities": [
                      {
                        "type": "BeeHive"
                      }
                    ],
                    "notification": {
                       "endpoint": {
                         "uri": "http://localhost:8084"
                       }
                    },
                    "jsonldContext": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-non-existing.jsonld"
                }
            """.trimIndent()

        val subscription = deserialize(
            rawSubscription.deserializeAsMap(),
            emptyList()
        ).shouldSucceedAndResult()
        subscription.validate()
            .shouldFail {
                assertInstanceOf(LdContextNotAvailableException::class.java, it)
            }
    }

    @Test
    fun `it should not validate a subscription with an invalid join level when join is flat or inline`() = runTest {
        val rawSubscription =
            """
                {
                    "id": "urn:ngsi-ld:Subscription:1234567890",
                    "type": "Subscription",
                    "entities": [
                      {
                        "type": "BeeHive"
                      }
                    ],
                    "notification": {
                       "endpoint": {
                         "uri": "http://localhost:8084"
                       },
                       "join": "flat",
                       "joinLevel": 0
                    }
                }
            """.trimIndent()

        val subscription = deserialize(rawSubscription.deserializeAsMap(), emptyList())
            .shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "The value of 'joinLevel' must be greater than zero (int) if 'join' is asked"
            }
    }

    @Test
    fun `it should not validate a subscription with simplified format and showChanges`() = runTest {
        val rawSubscription =
            """
                {
                    "id": "urn:ngsi-ld:Subscription:1234567890",
                    "type": "Subscription",
                    "entities": [
                      {
                        "type": "BeeHive"
                      }
                    ],
                    "notification": {
                       "endpoint": {
                         "uri": "http://localhost:8084"
                       },
                       "format": "simplified",
                       "showChanges": true
                    }
                }
            """.trimIndent()

        val subscription = deserialize(rawSubscription.deserializeAsMap(), emptyList())
            .shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "'showChanges' and 'simplified' / 'keyValues' format cannot be used at the same time"
            }
    }

    @Test
    fun `it should not validate a subscription with attributes and pick or omit`() = runTest {
        val rawSubscription =
            """
                {
                    "id": "urn:ngsi-ld:Subscription:1234567890",
                    "type": "Subscription",
                    "entities": [
                      {
                        "type": "BeeHive"
                      }
                    ],
                    "notification": {
                       "endpoint": {
                         "uri": "http://localhost:8084"
                       },
                       "attributes": ["$INCOMING_IRI"],
                       "pick": ["$INCOMING_IRI"]
                    }
                }
            """.trimIndent()

        val subscription = deserialize(rawSubscription.deserializeAsMap(), emptyList())
            .shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "'attributes' and 'pick' or 'omit' cannot be used at the same time"
            }
    }

    @Test
    fun `it should not validate a subscription with an intersection between pick and omit`() = runTest {
        val rawSubscription =
            """
                {
                    "id": "urn:ngsi-ld:Subscription:1234567890",
                    "type": "Subscription",
                    "entities": [
                      {
                        "type": "BeeHive"
                      }
                    ],
                    "notification": {
                       "endpoint": {
                         "uri": "http://localhost:8084"
                       },
                       "pick": ["$INCOMING_IRI"],
                       "omit": ["$INCOMING_IRI"]
                    }
                }
            """.trimIndent()

        val subscription = deserialize(rawSubscription.deserializeAsMap(), emptyList())
            .shouldSucceedAndResult()
        subscription.validate()
            .shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "An entity member cannot be present in both 'pick' and 'omit'"
            }
    }

    @Test
    fun `it should expand a subscription`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        assertThat(subscription)
            .extracting("geoQ.geoproperty", "watchedAttributes", "notification.attributes")
            .containsExactly(
                NGSILD_OPERATION_SPACE_IRI,
                listOf(INCOMING_IRI, OUTGOING_IRI),
                listOf(INCOMING_IRI)
            )
        assertThat(subscription.entities)
            .allMatch { it.typeSelection == BEEHIVE_IRI }
            .hasSize(1)
    }

    @Test
    fun `it should expand a subscription with a complex type selection`() {
        val subscription = subscription.copy(
            entities = setOf(
                EntitySelector(
                    id = null,
                    idPattern = null,
                    typeSelection = "($BEEHIVE_TERM,$APIARY_TERM);$BEEKEEPER_TERM"
                )
            )
        ).expand(APIC_COMPOUND_CONTEXTS)

        assertThat(subscription.entities)
            .allMatch { it.typeSelection == "($BEEHIVE_IRI,$APIARY_IRI);$BEEKEEPER_IRI" }
            .hasSize(1)
    }

    @Test
    fun `it should compact a subscription`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val compactedSubscription = subscription.compact(APIC_COMPOUND_CONTEXTS)

        assertThat(compactedSubscription)
            .extracting("geoQ.geoproperty", "watchedAttributes", "notification.attributes")
            .containsExactly(
                NGSILD_OPERATION_SPACE_TERM,
                listOf(INCOMING_TERM, OUTGOING_TERM),
                listOf(INCOMING_TERM)
            )
        assertThat(compactedSubscription.entities)
            .allMatch { it.typeSelection == BEEHIVE_TERM }
            .hasSize(1)
    }

    @Test
    fun `it should prepare a subscription as JSON and add the status attribute`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.prepareForRendering(NGSILD_TEST_CORE_CONTEXT, includeSysAttrs = false)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey("status"))
        assertEquals("active", deserializedSub["status"])
    }

    @Test
    fun `it should prepare a subscription as JSON without createdAt and modifiedAt if not specified`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.prepareForRendering(NGSILD_TEST_CORE_CONTEXT, includeSysAttrs = false)

        val deserializedSub = deserializeObject(serializedSub)

        assertFalse(deserializedSub.containsKey("createdAt"))
        assertFalse(deserializedSub.containsKey("modifiedAt"))
    }

    @Test
    fun `it should prepare a subscription as JSON with createdAt and modifiedAt if specified`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.prepareForRendering(NGSILD_TEST_CORE_CONTEXT, includeSysAttrs = true)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey("createdAt"))
    }

    @Test
    fun `it should prepare a subscription with a context if JSON-LD media type is asked`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.prepareForRendering(NGSILD_TEST_CORE_CONTEXT, mediaType = JSON_LD_MEDIA_TYPE)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey(JSONLD_CONTEXT_KW))
    }

    @Test
    fun `it should prepare a subscription without context if JSON media type is asked`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.prepareForRendering(
            NGSILD_TEST_CORE_CONTEXT,
            mediaType = MediaType.APPLICATION_JSON
        )

        val deserializedSub = deserializeObject(serializedSub)

        assertFalse(deserializedSub.containsKey(JSONLD_CONTEXT_KW))
    }

    @Test
    fun `it should prepare a list of subscriptions as JSON-LD with sysAttrs`() {
        val subscription = subscription.copy()
        val otherSubscription = subscription.copy(id = "urn:ngsi-ld:Subscription:02".toUri())

        val serializedSubs =
            listOf(subscription, otherSubscription).prepareForRendering(
                NGSILD_TEST_CORE_CONTEXTS,
                includeSysAttrs = true
            )

        val deserializedSubs = deserializeListOfObjects(serializedSubs)

        assertEquals(2, deserializedSubs.size)
        assertThat(deserializedSubs)
            .allMatch {
                it.containsKey(JSONLD_CONTEXT_KW) &&
                    it.containsKey(NGSILD_CREATED_AT_TERM)
            }
    }

    @Test
    fun `it should prepare a list of subscriptions as JSON without sysAttrs`() {
        val subscription = subscription.copy()
        val otherSubscription = subscription.copy(id = "urn:ngsi-ld:Subscription:02".toUri())

        val serializedSubs = listOf(subscription, otherSubscription)
            .prepareForRendering(NGSILD_TEST_CORE_CONTEXTS, mediaType = MediaType.APPLICATION_JSON)

        val deserializedSubs = deserializeListOfObjects(serializedSubs)

        assertEquals(2, deserializedSubs.size)
        assertThat(deserializedSubs)
            .allMatch {
                !it.containsKey(JSONLD_CONTEXT_KW) &&
                    !it.containsKey(NGSILD_CREATED_AT_TERM)
            }
    }
}
