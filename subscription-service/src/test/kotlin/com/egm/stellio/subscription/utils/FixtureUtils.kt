package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.GeoQuery
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.NotificationParams.*
import com.egm.stellio.subscription.model.Subscription
import java.time.Instant
import java.time.ZoneOffset

fun gimmeRawSubscription(
    withQueryAndGeoQuery: Pair<Boolean, Boolean> = Pair(true, true),
    withEndpointInfo: Boolean = true,
    withNotifParams: Pair<FormatType, List<String>> = Pair(FormatType.NORMALIZED, emptyList()),
    withModifiedAt: Boolean = false,
    georel: String = "within",
    coordinates: Any = "[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]"
): Subscription {
    val q =
        if (withQueryAndGeoQuery.first)
            "speed>50;foodName==dietary fibres"
        else
            null

    val geoQuery =
        if (withQueryAndGeoQuery.second)
            GeoQuery(
                georel = georel,
                geometry = GeoQuery.GeometryType.Polygon,
                coordinates = coordinates
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
            attributes = withNotifParams.second,
            format = withNotifParams.first,
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
}
