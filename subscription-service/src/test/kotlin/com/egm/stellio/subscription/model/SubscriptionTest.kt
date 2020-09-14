package com.egm.stellio.subscription.model

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.jayway.jsonpath.JsonPath.read
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset

class SubscriptionTest {

    private val endpointInfo =
        listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value"))

    private val modifiedSubscription = Subscription(
        id = "1",
        entities = emptySet(),
        createdAt = Instant.now().atZone(ZoneOffset.UTC),
        modifiedAt = Instant.now().atZone(ZoneOffset.UTC),
        notification = NotificationParams(
            attributes = listOf("incoming"),
            format = NotificationParams.FormatType.KEY_VALUES,
            endpoint = Endpoint(
                uri = URI.create("http://localhost:8089/notification"),
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
    fun `it should serialize Subscription as JSON without createdAt and modifiedAt if not specified`() {
        val serializedSub = modifiedSubscription.toJson(false)
        assertFalse(serializedSub.contains("createdAt"))
        assertFalse(serializedSub.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize Subscription as JSON with createdAt and modifiedAt if specified`() {
        val serializedSubscription = modifiedSubscription.toJson(true)
        assertTrue(serializedSubscription.contains("createdAt"))
        assertTrue(serializedSubscription.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON without createdAt and modifiedAt if not specified`() {
        val otherModifiedSubscription = modifiedSubscription.copy(id = "2")
        val serializedSubscription = listOf(modifiedSubscription, otherModifiedSubscription).toJson()

        assertFalse(serializedSubscription.contains("createdAt"))
        assertFalse(serializedSubscription.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON with createdAt and modifiedAt if specified`() {
        val otherModifiedSubscription = modifiedSubscription.copy(id = "2")
        val serializedSubscriptions = listOf(modifiedSubscription, otherModifiedSubscription).toJson(true)
        with(serializedSubscriptions) {
            assertTrue(read<List<String>>(this, "$[*].createdAt").size == 2)
            assertTrue(read<List<String>>(this, "$[*].modifiedAt").size == 2)
        }
    }
}
