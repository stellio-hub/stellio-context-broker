package com.egm.stellio.subscription.support

import com.egm.stellio.shared.model.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.GeoQ
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.utils.ParsingUtils

fun loadAndDeserializeSubscription(filename: String, contexts: List<String> = APIC_COMPOUND_CONTEXTS): Subscription {
    val subscriptionPayload = loadSampleData(filename)
    return ParsingUtils.parseSubscription(subscriptionPayload.deserializeAsMap(), contexts)
        .shouldSucceedAndResult()
}

fun gimmeSubscriptionFromMembers(
    additionalMembers: Map<String, Any>,
    contexts: List<String> = APIC_COMPOUND_CONTEXTS
): Subscription {
    val payload = mapOf(
        "id" to "urn:ngsi-ld:Subscription:01".toUri(),
        "type" to NGSILD_SUBSCRIPTION_TERM,
        "notification" to mapOf("endpoint" to mapOf("uri" to "http://my.endpoint/notifiy"))
    ).plus(additionalMembers)

    return ParsingUtils.parseSubscription(payload, contexts).shouldSucceedAndResult()
}

fun gimmeRawSubscription(
    withQueryAndGeoQuery: Pair<Boolean, Boolean> = Pair(true, true),
    withEndpointReceiverInfo: Boolean = true,
    withNotifParams: Pair<FormatType, List<String>> = Pair(FormatType.NORMALIZED, emptyList()),
    georel: String = "within",
    geometry: String = "Polygon",
    coordinates: String = "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]",
    timeInterval: Int? = null,
    contexts: List<String> = listOf(NGSILD_TEST_CORE_CONTEXT)
): Subscription {
    val q =
        if (withQueryAndGeoQuery.first)
            "speed>50;foodName==dietary fibres"
        else
            null

    val geoQ =
        if (withQueryAndGeoQuery.second)
            GeoQ(
                georel = georel,
                geometry = geometry,
                coordinates = coordinates,
                pgisGeometry = "100000101000101010100010100054300"
            )
        else
            null

    val endpointReceiverInfo =
        if (withEndpointReceiverInfo)
            listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value"))
        else
            null

    return Subscription(
        type = NGSILD_SUBSCRIPTION_TERM,
        subscriptionName = "My Subscription",
        description = "My beautiful subscription",
        q = q,
        timeInterval = timeInterval,
        entities = emptySet(),
        geoQ = geoQ,
        notification = NotificationParams(
            attributes = withNotifParams.second,
            format = withNotifParams.first,
            endpoint = Endpoint(
                uri = "http://localhost:8089/notification".toUri(),
                accept = Endpoint.AcceptType.JSONLD,
                receiverInfo = endpointReceiverInfo
            )
        ),
        contexts = contexts
    )
}
