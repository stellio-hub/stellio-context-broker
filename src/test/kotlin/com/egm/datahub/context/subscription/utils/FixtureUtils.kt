package com.egm.datahub.context.subscription.utils

import com.egm.datahub.context.subscription.model.Endpoint
import com.egm.datahub.context.subscription.model.GeoQuery
import com.egm.datahub.context.subscription.model.NotificationParams
import com.egm.datahub.context.subscription.model.Subscription
import java.net.URI

fun gimmeRawSubscription(withGeoQuery: Boolean = true, georel: String = "within"): Subscription {
    val geoQuery =
            if (withGeoQuery)
                GeoQuery(georel = georel, geometry = GeoQuery.GeometryType.Polygon, coordinates = "[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]")
            else
                null

    return Subscription(name = "My Subscription",
        description = "My beautiful subscription",
        entities = emptySet(),
        geoQ = geoQuery,
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