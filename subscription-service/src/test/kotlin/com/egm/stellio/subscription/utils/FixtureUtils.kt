package com.egm.stellio.subscription.utils

import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.GeoQuery
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.Subscription
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset

fun gimmeRawSubscription(
    withQuery: Boolean = true,
    withGeoQuery: Boolean = true,
    withEndpointInfo: Boolean = true,
    withModifiedAt: Boolean = false,
    georel: String = "within"
): Subscription {
    val q =
        if (withQuery)
            "speed>50;foodName==dietary fibres"
        else
            null

    val geoQuery =
        if (withGeoQuery)
            GeoQuery(
                georel = georel,
                geometry = GeoQuery.GeometryType.Polygon,
                coordinates = "[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]"
            )
        else
            null

    val endpointInfo =
        if (withEndpointInfo)
            listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value"))
        else
            null

    val modifiedAtValue = if (withModifiedAt) Instant.now().atZone(ZoneOffset.UTC) else null
    return Subscription(
        name = "My Subscription",
        modifiedAt = modifiedAtValue,
        description = "My beautiful subscription",
        q = q,
        entities = emptySet(),
        geoQ = geoQuery,
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
}
