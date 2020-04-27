package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.NgsiLdParsingUtils
import org.springframework.data.annotation.Id
import java.util.*

data class Subscription(
    @Id val id: String = "urn:ngsi-ld:Subscription:${UUID.randomUUID()}",
    val type: String = "Subscription",
    val name: String? = null,
    val description: String? = null,
    val entities: Set<EntityInfo>,
    val q: String? = null,
    val geoQ: GeoQuery? = null,
    val notification: NotificationParams,
    val isActive: Boolean = true
) {
    fun expandTypes(context: List<String>) {
        this.entities.forEach {
            it.type = NgsiLdParsingUtils.expandJsonLdKey(it.type, context) !!
        }
        this.notification.attributes = this.notification.attributes.map {
            NgsiLdParsingUtils.expandJsonLdKey(it, context) !!
        }
    }
}