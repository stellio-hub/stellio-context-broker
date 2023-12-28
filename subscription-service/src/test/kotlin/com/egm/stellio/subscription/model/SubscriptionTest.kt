package com.egm.stellio.subscription.model

import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
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

    private val endpointReceiverInfo =
        listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value"))

    private val subscription = Subscription(
        id = "urn:ngsi-ld:Subscription:01".toUri(),
        type = NGSILD_SUBSCRIPTION_TERM,
        entities = setOf(
            EntitySelector(
                id = null,
                idPattern = null,
                typeSelection = BEEHIVE_TYPE
            )
        ),
        geoQ = GeoQ(
            georel = "within",
            geometry = "Polygon",
            coordinates = "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]",
            pgisGeometry = "100000101000101010100010100054300",
            geoproperty = NGSILD_OPERATION_SPACE_PROPERTY
        ),
        watchedAttributes = listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
        notification = NotificationParams(
            attributes = listOf(INCOMING_PROPERTY),
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
    fun `it should expand a subscription`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        assertThat(subscription)
            .extracting("geoQ.geoproperty", "watchedAttributes", "notification.attributes")
            .containsExactly(
                NGSILD_OPERATION_SPACE_PROPERTY,
                listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                listOf(INCOMING_PROPERTY)
            )
        assertThat(subscription.entities)
            .allMatch { it.typeSelection == BEEHIVE_TYPE }
            .hasSize(1)
    }

    @Test
    fun `it should expand a subscription with a complex type selection`() {
        val subscription = subscription.copy(
            entities = setOf(
                EntitySelector(
                    id = null,
                    idPattern = null,
                    typeSelection = "($BEEHIVE_COMPACT_TYPE,$APIARY_COMPACT_TYPE);$BEEKEEPER_COMPACT_TYPE"
                )
            )
        ).expand(APIC_COMPOUND_CONTEXTS)

        assertThat(subscription.entities)
            .allMatch { it.typeSelection == "($BEEHIVE_TYPE,$APIARY_TYPE);$BEEKEEPER_TYPE" }
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
                listOf(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY),
                listOf(INCOMING_COMPACT_PROPERTY)
            )
        assertThat(compactedSubscription.entities)
            .allMatch { it.typeSelection == BEEHIVE_COMPACT_TYPE }
            .hasSize(1)
    }

    @Test
    fun `it should serialize a subscription as JSON and add the status attribute`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.serialize(NGSILD_TEST_CORE_CONTEXT, includeSysAttrs = false)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey("status"))
        assertEquals("active", deserializedSub["status"])
    }

    @Test
    fun `it should serialize a subscription as JSON without createdAt and modifiedAt if not specified`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.serialize(NGSILD_TEST_CORE_CONTEXT, includeSysAttrs = false)

        val deserializedSub = deserializeObject(serializedSub)

        assertFalse(deserializedSub.containsKey("createdAt"))
        assertFalse(deserializedSub.containsKey("modifiedAt"))
    }

    @Test
    fun `it should serialize a subscription as JSON with createdAt and modifiedAt if specified`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.serialize(NGSILD_TEST_CORE_CONTEXT, includeSysAttrs = true)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey("createdAt"))
    }

    @Test
    fun `it should serialize a subscription with a context if JSON-LD media type is asked`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.serialize(NGSILD_TEST_CORE_CONTEXT, mediaType = JSON_LD_MEDIA_TYPE)

        val deserializedSub = deserializeObject(serializedSub)

        assertTrue(deserializedSub.containsKey(JSONLD_CONTEXT))
    }

    @Test
    fun `it should serialize a subscription without context if JSON media type is asked`() {
        val subscription = subscription.copy().expand(APIC_COMPOUND_CONTEXTS)

        val serializedSub = subscription.serialize(NGSILD_TEST_CORE_CONTEXT, mediaType = MediaType.APPLICATION_JSON)

        val deserializedSub = deserializeObject(serializedSub)

        assertFalse(deserializedSub.containsKey(JSONLD_CONTEXT))
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON-LD with sysAttrs`() {
        val subscription = subscription.copy()
        val otherSubscription = subscription.copy(id = "urn:ngsi-ld:Subscription:02".toUri())

        val serializedSubs =
            listOf(subscription, otherSubscription).serialize(NGSILD_TEST_CORE_CONTEXTS, includeSysAttrs = true)

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
            .serialize(NGSILD_TEST_CORE_CONTEXTS, mediaType = MediaType.APPLICATION_JSON)

        val deserializedSubs = deserializeListOfObjects(serializedSubs)

        assertEquals(2, deserializedSubs.size)
        assertThat(deserializedSubs)
            .allMatch {
                !it.containsKey(JSONLD_CONTEXT) &&
                    !it.containsKey(NGSILD_CREATED_AT_TERM)
            }
    }
}
