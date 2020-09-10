package com.egm.stellio.subscription.model

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
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
    fun `it should initialize the createdAt and modifiedAt attributes upon Subscription creation`() {
        assertNotNull(modifiedSubscription.createdAt)
        assertNotNull(modifiedSubscription.modifiedAt)
    }

    @Test
    fun `it should serialize Subscription as JSON without createdAt and modifiedAt if not specified`() {
        val serializedSub = modifiedSubscription.toJson(false)
        assertFalse(serializedSub.contains("createdAt"))
        assertFalse(serializedSub.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize Subscription as JSON with createdAt and modifiedAt if specified`() {
        val serializedSub = modifiedSubscription.toJson(true)
        assertTrue(serializedSub.contains("createdAt"))
        assertTrue(serializedSub.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON without createdAt and modifiedAt if not specified`() {
        val otherModifiedSubscription = modifiedSubscription.copy(id = "2")
        val serializedSubs = Subscription.toJson(listOf(modifiedSubscription, otherModifiedSubscription), false)
        assertFalse(serializedSubs.contains("createdAt"))
        assertFalse(serializedSubs.contains("modifiedAt"))
    }

    @Test
    fun `it should serialize a list of subscriptions as JSON with createdAt and modifiedAt if specified`() {
        val otherModifiedSubscription = modifiedSubscription.copy(id = "2")
        val serializedSubs = Subscription.toJson(listOf(modifiedSubscription, otherModifiedSubscription), true)
        assertTrue(serializedSubs.contains("createdAt"))
        assertTrue(serializedSubs.contains("modifiedAt"))
    }
}
