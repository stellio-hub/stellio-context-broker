package com.egm.datahub.context.subscription.utils

import com.egm.datahub.context.subscription.model.Endpoint
import com.egm.datahub.context.subscription.model.NotificationParams
import com.egm.datahub.context.subscription.model.Subscription
import java.net.URI

fun gimmeRawSubscription(): Subscription {
    return Subscription(name = "My Subscription",
        description = "My beautiful subscription",
        entities = emptySet(),
        notification = NotificationParams(
            attributes = listOf("incoming"),
            format = NotificationParams.FormatType.KEY_VALUES,
            endpoint = Endpoint(
                uri = URI.create("http://localhost:8089/notification"),
                accept = Endpoint.AcceptType.JSONLD
            ),
            status = null,
            lastNotification = null,
            lastFailure = null,
            lastSuccess = null
        )
    )
}