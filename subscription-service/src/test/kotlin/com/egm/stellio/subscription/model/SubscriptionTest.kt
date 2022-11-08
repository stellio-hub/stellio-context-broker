package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OPERATION_SPACE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OPERATION_SPACE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.jayway.jsonpath.JsonPath.read
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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

        val serializedSub = subscription.compact(APIC_COMPOUND_CONTEXT)

        assertThat(serializedSub)
            .extracting("geoQ.geoproperty", "watchedAttributes", "notification.attributes")
            .containsExactly(
                NGSILD_OPERATION_SPACE_TERM,
                listOf(INCOMING_COMPACT_PROPERTY, OUTGOING_COMPACT_PROPERTY),
                listOf(INCOMING_COMPACT_PROPERTY)
            )
        assertThat(subscription.entities)
            .allMatch { it.type == BEEHIVE_COMPACT_TYPE }
            .hasSize(1)
//
//        assertFalse(serializedSub.contains("https://uri.etsi.org/ngsi-ld/default-context/type"))
//        assertFalse(serializedSub.contains("https://uri.etsi.org/ngsi-ld/default-context/incoming"))
//        assertTrue(serializedSub.contains("incoming"))
//        assertFalse(serializedSub.contains("https://uri.etsi.org/ngsi-ld/operationSpace"))
//        assertTrue(serializedSub.contains("operationSpace"))
    }

    @Test
    fun `it should serialize a subscription as JSON without createdAt and modifiedAt if not specified`() {
        val serializedSub = subscription.toJson(NGSILD_CORE_CONTEXT, includeSysAttrs = false)
        assertFalse(serializedSub.contains("createdAt"))
        assertFalse(serializedSub.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize a subscription as JSON with createdAt and modifiedAt if specified`() {
        val serializedSubscription = subscription.toJson(NGSILD_CORE_CONTEXT, includeSysAttrs = true)
        assertTrue(serializedSubscription.contains("createdAt"))
        assertTrue(serializedSubscription.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize a subscription with a context if JSON-LD media type is asked`() {
        val serializedSubscription = subscription.toJson(NGSILD_CORE_CONTEXT, includeSysAttrs = true)
        assertTrue(serializedSubscription.contains("@context"))
        assertFalse(serializedSubscription.contains("contexts"))
    }

    @Test
    fun `it should serialize a subscription without context if JSON media type is asked`() {
        val serializedSubscription =
            subscription.toJson(NGSILD_CORE_CONTEXT, MediaType.APPLICATION_JSON, includeSysAttrs = true)
        assertFalse(serializedSubscription.contains("@context"))
        assertFalse(serializedSubscription.contains("contexts"))
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON without createdAt and modifiedAt if not specified`() {
        val otherSubscription = subscription.copy(id = "urn:ngsi-ld:Subscription:02".toUri())
        val serializedSubscription = listOf(subscription, otherSubscription).toJson(NGSILD_CORE_CONTEXT)

        assertFalse(serializedSubscription.contains("createdAt"))
        assertFalse(serializedSubscription.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON with createdAt and modifiedAt if specified`() {
        val otherSubscription = subscription.copy(id = "urn:ngsi-ld:Subscription:02".toUri())
        val serializedSubscriptions =
            listOf(subscription, otherSubscription).toJson(NGSILD_CORE_CONTEXT, includeSysAttrs = true)
        with(serializedSubscriptions) {
            assertTrue(read<List<String>>(this, "$[*].createdAt").size == 2)
            assertTrue(read<List<String>>(this, "$[*].modifiedAt").size == 2)
        }
    }
}
