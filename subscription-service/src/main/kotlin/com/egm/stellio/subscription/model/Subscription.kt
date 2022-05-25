package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.addContextToElement
import com.egm.stellio.shared.util.JsonLdUtils.addContextToListOfElements
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonFilter
import org.springframework.data.annotation.Id
import org.springframework.http.MediaType
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

data class Subscription(
    @Id val id: URI = "urn:ngsi-ld:Subscription:${UUID.randomUUID()}".toUri(),
    val type: String = "Subscription",
    val subscriptionName: String? = null,
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),
    val modifiedAt: ZonedDateTime? = null,
    val description: String? = null,
    val entities: Set<EntityInfo>,
    val watchedAttributes: List<String>? = null,
    val timeInterval: Int? = null,
    val q: String? = null,
    val geoQ: GeoQuery? = null,
    val notification: NotificationParams,
    val isActive: Boolean = true,
    val expiresAt: ZonedDateTime? = null
) {
    fun expandTypes(context: List<String>) {
        this.entities.forEach {
            it.type = JsonLdUtils.expandJsonLdTerm(it.type, context)!!
        }
        this.notification.attributes = this.notification.attributes?.map {
            JsonLdUtils.expandJsonLdTerm(it, context)!!
        }
        this.geoQ?.geoproperty = this.geoQ?.geoproperty?.let { JsonLdUtils.expandJsonLdTerm(it, context) }
    }

    fun compact(contexts: List<String>): Subscription =
        this.copy(
            entities = entities.map {
                EntityInfo(it.id, it.idPattern, compactTerm(it.type, contexts))
            }.toSet(),
            notification = notification.copy(
                attributes = notification.attributes?.map { compactTerm(it, contexts) }
            ),
            geoQ = geoQ?.copy(
                geoproperty = geoQ.geoproperty?.let { compactTerm(it, contexts) }
            )
        )

    fun compact(context: String): Subscription =
        compact(listOf(context))

    fun toJson(
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String {
        val serializedSubscription = if (includeSysAttrs)
            JsonUtils.serializeObject(this.compact(contexts))
        else
            serializeWithoutSysAttrs(this.compact(contexts))
        return if (mediaType == JSON_LD_MEDIA_TYPE)
            addContextToElement(serializedSubscription, contexts)
        else
            serializedSubscription
    }

    fun toJson(context: String, mediaType: MediaType = JSON_LD_MEDIA_TYPE, includeSysAttrs: Boolean = false): String =
        toJson(listOf(context), mediaType, includeSysAttrs)
}

fun List<Subscription>.toJson(
    context: String,
    mediaType: MediaType = JSON_LD_MEDIA_TYPE,
    includeSysAttrs: Boolean = false
): String {
    val compactedSubscriptions = this.map { it.compact(context) }
    val serializedSubscriptions = if (includeSysAttrs)
        JsonUtils.serializeObject(compactedSubscriptions)
    else
        serializeWithoutSysAttrs(compactedSubscriptions)
    return if (mediaType == JSON_LD_MEDIA_TYPE)
        addContextToListOfElements(serializedSubscriptions, listOf(context))
    else
        serializedSubscriptions
}

private fun serializeWithoutSysAttrs(input: Any) =
    JsonUtils.serializeObject(
        input,
        Subscription::class,
        SysAttrsMixinFilter::class,
        "sysAttrs",
        setOf("createdAt", "modifiedAt")
    )

@JsonFilter("sysAttrs")
class SysAttrsMixinFilter
