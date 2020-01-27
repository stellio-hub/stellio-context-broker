package com.egm.datahub.context.subscription.model

import org.springframework.data.annotation.Id
import java.util.*

data class Subscription(
    @Id val id: String = "urn:ngsi-ld:Subscription:${UUID.randomUUID()}",
    val type: String = "Subscription",
    val name: String? = null,
    val description: String? = null,
    val entities: Set<EntityInfo>,
    val notification: NotificationParams
)