package com.egm.stellio.subscription.websocket

import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.Subscription

class WSSubscription(
    val body: Subscription,
    val link: String,
    // val NGSILD-Tenant: String,

)

data class WSNotification(
    val body: Notification,
    val metadata: Map<String, String> = emptyMap(),
)
