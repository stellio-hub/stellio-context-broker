package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils
import com.fasterxml.jackson.annotation.JsonFilter
import org.springframework.data.annotation.Id
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

data class Subscription(
    @Id val id: URI = URI.create("urn:ngsi-ld:Subscription:${UUID.randomUUID()}"),
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
        this.notification.attributes = this.notification.attributes.map {
            JsonLdUtils.expandJsonLdKey(it, context)!!
        }
    }

    fun toJson(includeSysAttrs: Boolean = false): String {
        return if (includeSysAttrs)
            JsonUtils.serializeObject(this)
        else
            serializeWithoutSysAttrs(this)
    }
}

fun List<Subscription>.toJson(includeSysAttrs: Boolean = false): String {
    return if (includeSysAttrs)
        JsonUtils.serializeObject(this)
    else
        serializeWithoutSysAttrs(this)
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
