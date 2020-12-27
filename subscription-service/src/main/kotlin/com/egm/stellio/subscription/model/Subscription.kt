package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonFilter
import org.springframework.data.annotation.Id
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

data class Subscription(
    @Id val id: URI = "urn:ngsi-ld:Subscription:${UUID.randomUUID()}".toUri(),
    val type: String = "Subscription",
    val name: String? = null,
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),
    val modifiedAt: ZonedDateTime? = null,
    val description: String? = null,
    val entities: Set<EntityInfo>,
    val watchedAttributes: List<String>? = null,
    val q: String? = null,
    val geoQ: GeoQuery? = null,
    val notification: NotificationParams,
    val isActive: Boolean = true
) {
    fun expandTypes(context: List<String>) {
        this.entities.forEach {
            it.type = JsonLdUtils.expandJsonLdKey(it.type, context)!!
        }
        this.notification.attributes = this.notification.attributes?.map {
            JsonLdUtils.expandJsonLdKey(it, context)!!
        }
    }

    fun compact(context: String): Subscription =
        this.copy(
            entities = entities.map {
                EntityInfo(it.id, it.idPattern, compactTerm(it.type, listOf(context)))
            }.toSet(),
            notification = notification.copy(
                attributes = notification.attributes?.map { compactTerm(it, listOf(context)) }
            )
        )

    fun toJson(context: String, includeSysAttrs: Boolean = false): String {
        return if (includeSysAttrs)
            JsonUtils.serializeObject(this.compact(context))
        else
            serializeWithoutSysAttrs(this.compact(context))
    }
}

fun List<Subscription>.toJson(context: String, includeSysAttrs: Boolean = false): String {
    val compactedSubscriptions = this.map { it.compact(context) }
    return if (includeSysAttrs)
        JsonUtils.serializeObject(compactedSubscriptions)
    else
        serializeWithoutSysAttrs(compactedSubscriptions)
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
