package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OPERATION_SPACE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OPERATION_SPACE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.JsonUtils.deserializeListOfObjects
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType

class SubscriptionTest {

    private val endpointInfo =
        listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value"))

    private val subscription = Subscription(
        id = "urn:ngsi-ld:Subscription:01".toUri(),
        type = NGSILD_SUBSCRIPTION_TERM,
        entities = setOf(
            EntityInfo(
                id = null,
                idPattern = null,
                type = BEEHIVE_COMPACT_TYPE
            )
        ),
        geoQ = GeoQuery(
            georel = "within",
            geometry = "Polygon",
            coordinates = "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]",
            pgisGeometry = "100000101000101010100010100054300",
            geoproperty = "operationSpace"
        ),
        watchedAttributes = listOf(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY),
        notification = NotificationParams(
            attributes = listOf(INCOMING_COMPACT_PROPERTY),
            format = NotificationParams.FormatType.KEY_VALUES,
            endpoint = Endpoint(
                uri = "http://localhost:8089/notification".toUri(),
                accept = Endpoint.AcceptType.JSONLD,
                info = endpointInfo
            )
        ),
        contexts = listOf(APIC_COMPOUND_CONTEXT)
    )

    @Test
    fun `it should expand a subscription`() {
        val subscription = subscription.copy()
        subscription.expand(listOf(APIC_COMPOUND_CONTEXT))

        assertThat(subscription)
            .extracting("geoQ.geoproperty", "watchedAttributes", "notification.attributes")
            .containsExactly(
                NGSILD_OPERATION_SPACE_PROPERTY,
                listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                listOf(INCOMING_PROPERTY)
            )
        assertThat(subscription.entities)
            .allMatch { it.type == BEEHIVE_TYPE }
            .hasSize(1)
    }

    @Test
    fun `it should compact a subscription`() {
        val subscription = subscription.copy()
        subscription.expand(listOf(APIC_COMPOUND_CONTEXT))

        val compactedSubscription = subscription.compact(APIC_COMPOUND_CONTEXT)

        assertThat(compactedSubscription)
            .extracting("geoQ.geoproperty", "watchedAttributes", "notification.attributes")
            .containsExactly(
                NGSILD_OPERATION_SPACE_TERM,
                listOf(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY),
                listOf(INCOMING_COMPACT_PROPERTY)
            )
        assertThat(compactedSubscription.entities)
            .allMatch { it.type == BEEHIVE_COMPACT_TYPE }
            .hasSize(1)
    }

    @Test
    fun `it should serialize a subscription as JSON and add the status attribute`() {
        val subscription = subscription.copy()
        subscription.expand(listOf(APIC_COMPOUND_CONTEXT))

        val serializedSub = subscription.serialize(NGSILD_CORE_CONTEXT, includeSysAttrs = false)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey("status"))
        assertEquals("active", deserializedSub["status"])
    }

    @Test
    fun `it should serialize a subscription as JSON without createdAt and modifiedAt if not specified`() {
        val subscription = subscription.copy()
        subscription.expand(listOf(APIC_COMPOUND_CONTEXT))

        val serializedSub = subscription.serialize(NGSILD_CORE_CONTEXT, includeSysAttrs = false)

        val deserializedSub = deserializeObject(serializedSub)

        assertFalse(deserializedSub.containsKey("createdAt"))
        assertFalse(deserializedSub.containsKey("modifiedAt"))
    }

    @Test
    fun `it should serialize a subscription as JSON with createdAt and modifiedAt if specified`() {
        val subscription = subscription.copy()
        subscription.expand(listOf(APIC_COMPOUND_CONTEXT))

        val serializedSub = subscription.serialize(NGSILD_CORE_CONTEXT, includeSysAttrs = true)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey("createdAt"))
    }

    @Test
    fun `it should serialize a subscription with a context if JSON-LD media type is asked`() {
        val subscription = subscription.copy()
        subscription.expand(listOf(APIC_COMPOUND_CONTEXT))

        val serializedSub = subscription.serialize(NGSILD_CORE_CONTEXT, mediaType = JSON_LD_MEDIA_TYPE)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey(JSONLD_CONTEXT))
    }

    @Test
    fun `it should serialize a subscription without context if JSON media type is asked`() {
        val subscription = subscription.copy()
        subscription.expand(listOf(APIC_COMPOUND_CONTEXT))

        val serializedSub = subscription.serialize(NGSILD_CORE_CONTEXT, mediaType = MediaType.APPLICATION_JSON)

        val deserializedSub = deserializeObject(serializedSub)

        assertFalse(deserializedSub.containsKey(JSONLD_CONTEXT))
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON-LD with sysAttrs`() {
        val subscription = subscription.copy()
        val otherSubscription = subscription.copy(id = "urn:ngsi-ld:Subscription:02".toUri())

        val serializedSubs =
            listOf(subscription, otherSubscription).serialize(NGSILD_CORE_CONTEXT, includeSysAttrs = true)

        val deserializedSubs = deserializeListOfObjects(serializedSubs)

        assertEquals(2, deserializedSubs.size)
        assertThat(deserializedSubs)
            .allMatch {
                it.containsKey(JSONLD_CONTEXT) &&
                    it.containsKey(NGSILD_CREATED_AT_TERM)
            }
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON without sysAttrs`() {
        val subscription = subscription.copy()
        val otherSubscription = subscription.copy(id = "urn:ngsi-ld:Subscription:02".toUri())

        val serializedSubs = listOf(subscription, otherSubscription)
            .serialize(NGSILD_CORE_CONTEXT, mediaType = MediaType.APPLICATION_JSON)

        val deserializedSubs = deserializeListOfObjects(serializedSubs)

        assertEquals(2, deserializedSubs.size)
        assertThat(deserializedSubs)
            .allMatch {
                !it.containsKey(JSONLD_CONTEXT) &&
                    !it.containsKey(NGSILD_CREATED_AT_TERM)
            }
    }
}
