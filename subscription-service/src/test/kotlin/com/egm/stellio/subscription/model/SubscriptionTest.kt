package com.egm.stellio.subscription.model

import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_OPERATION_SPACE_IRI
import com.egm.stellio.shared.model.NGSILD_OPERATION_SPACE_TERM
import com.egm.stellio.shared.model.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.BEEKEEPER_IRI
import com.egm.stellio.shared.util.BEEKEEPER_TERM
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeListOfObjects
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.OUTGOING_TERM
import com.egm.stellio.shared.util.toUri
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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
