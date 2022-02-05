package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.toUri
import com.jayway.jsonpath.JsonPath.read
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import java.time.Instant
import java.time.ZoneOffset

class SubscriptionTest {

    private val endpointInfo =
        listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value"))

    private val subscription = Subscription(
        id = "urn:ngsi-ld:Subscription:01".toUri(),
        entities = setOf(
            EntityInfo(
                id = null,
                idPattern = null,
                type = "https://uri.etsi.org/ngsi-ld/default-context/type"
            )
        ),
        createdAt = Instant.now().atZone(ZoneOffset.UTC),
        modifiedAt = Instant.now().atZone(ZoneOffset.UTC),
        notification = NotificationParams(
            attributes = listOf("https://uri.etsi.org/ngsi-ld/default-context/incoming"),
            format = NotificationParams.FormatType.KEY_VALUES,
            endpoint = Endpoint(
                uri = "http://localhost:8089/notification".toUri(),
                accept = Endpoint.AcceptType.JSONLD,
                info = endpointInfo
            ),
            status = null,
            lastNotification = null,
            lastFailure = null,
            lastSuccess = null
        )
    )

    @Test
    fun `it should compact a subscription when serializing as JSON`() {
        val serializedSub = subscription.toJson(NGSILD_CORE_CONTEXT, includeSysAttrs = false)
        assertFalse(serializedSub.contains("https://uri.etsi.org/ngsi-ld/default-context/type"))
        assertFalse(serializedSub.contains("https://uri.etsi.org/ngsi-ld/default-context/incoming"))
        assertTrue(serializedSub.contains("incoming"))
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
    }

    @Test
    fun `it should serialize a subscription without context if JSON media type is asked`() {
        val serializedSubscription =
            subscription.toJson(NGSILD_CORE_CONTEXT, MediaType.APPLICATION_JSON, includeSysAttrs = true)
        assertFalse(serializedSubscription.contains("@context"))
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
