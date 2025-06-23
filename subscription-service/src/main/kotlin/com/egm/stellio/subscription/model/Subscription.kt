package com.egm.stellio.subscription.model

import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.DataTypes.convertTo
import com.egm.stellio.shared.util.DataTypes.serialize
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.compactTypeSelection
import com.egm.stellio.shared.util.expandTypeSelection
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toFinalRepresentation
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.NotificationTrigger.ATTRIBUTE_CREATED
import com.egm.stellio.subscription.model.NotificationTrigger.ATTRIBUTE_UPDATED
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.http.MediaType
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

val defaultNotificationTriggers = listOf(
    ATTRIBUTE_CREATED.notificationTrigger,
    ATTRIBUTE_UPDATED.notificationTrigger
)

data class Subscription(
    @Id val id: URI = "urn:ngsi-ld:Subscription:${UUID.randomUUID()}".toUri(),
    val type: String,
    val subscriptionName: String? = null,
    val createdAt: ZonedDateTime = ngsiLdDateTime(),
    val modifiedAt: ZonedDateTime = createdAt,
    val description: String? = null,
    val entities: Set<EntitySelector>?,
    val watchedAttributes: List<ExpandedTerm>? = null,
    val notificationTrigger: List<String> = defaultNotificationTriggers,
    val timeInterval: Int? = null,
    val q: String? = null,
    val geoQ: GeoQ? = null,
    val scopeQ: String? = null,
    val notification: NotificationParams,
    @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = JsonBooleanFilter::class)
    val isActive: Boolean = true,
    val expiresAt: ZonedDateTime? = null,
    // creation time contexts:
    //  - used to compact entities in notifications
    //  - used when needed to serve contexts in JSON notifications
    @JsonProperty(value = JSONLD_CONTEXT)
    val contexts: List<ExpandedTerm>,
    val throttling: Int? = null,
    val lang: String? = null,
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    val datasetId: List<String>? = null,
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    val jsonldContext: URI? = null
) {

    @Transient
    val status: SubscriptionStatus =
        if (!isActive)
            SubscriptionStatus.PAUSED
        else if (expiresAt != null && expiresAt.isBefore(ngsiLdDateTime()))
            SubscriptionStatus.EXPIRED
        else
            SubscriptionStatus.ACTIVE

    fun expand(contexts: List<String>): Subscription =
        this.copy(
            entities = entities?.map { entitySelector ->
                entitySelector.copy(
                    typeSelection = expandTypeSelection(entitySelector.typeSelection, contexts)!!
                )
            }?.toSet(),
            notification = notification.copy(
                attributes = notification.attributes?.map { attributeName ->
                    expandJsonLdTerm(attributeName, contexts)
                }
            ),
            geoQ = geoQ?.copy(
                geoproperty = expandJsonLdTerm(geoQ.geoproperty, contexts)
            ),
            watchedAttributes = watchedAttributes?.map { attributeName ->
                expandJsonLdTerm(attributeName, contexts)
            }
        )

    fun compact(contexts: List<String>): Subscription =
        this.copy(
            entities = entities?.map {
                EntitySelector(it.id, it.idPattern, compactTypeSelection(it.typeSelection, contexts))
            }?.toSet(),
            notification = notification.copy(
                attributes = notification.attributes?.map { compactTerm(it, contexts) }
            ),
            geoQ = geoQ?.copy(
                geoproperty = compactTerm(geoQ.geoproperty, contexts)
            ),
            watchedAttributes = this.watchedAttributes?.map { compactTerm(it, contexts) }
        )

    fun prepareForRendering(
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String =
        convertTo<Map<String, Any>>(this.compact(contexts))
            .toFinalRepresentation(mediaType, includeSysAttrs)
            .let { serialize(it) }

    fun prepareForRendering(
        context: String,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String =
        prepareForRendering(listOf(context), mediaType, includeSysAttrs)
}

// Default for booleans is false, so add a simple filter to only include "isActive" is it is false
// see https://github.com/FasterXML/jackson-databind/issues/1331 for instance
class JsonBooleanFilter {

    override fun equals(other: Any?): Boolean {
        if (other == null || other !is Boolean) {
            return false
        }

        return other == true
    }

    override fun hashCode(): Int = javaClass.hashCode()
}

enum class SubscriptionStatus(val status: String) {
    @JsonProperty("active")
    ACTIVE("active"),

    @JsonProperty("paused")
    PAUSED("paused"),

    @JsonProperty("expired")
    EXPIRED("expired")
}

enum class NotificationTrigger(val notificationTrigger: String) {
    ENTITY_CREATED("entityCreated"),
    ENTITY_UPDATED("entityUpdated"),
    ENTITY_DELETED("entityDeleted"),
    ATTRIBUTE_CREATED("attributeCreated"),
    ATTRIBUTE_UPDATED("attributeUpdated"),
    ATTRIBUTE_DELETED("attributeDeleted");

    companion object {
        fun isValid(notificationTrigger: String): Boolean =
            NotificationTrigger.entries.any { it.notificationTrigger == notificationTrigger }

        fun expandEntityUpdated(): String =
            listOf(
                ATTRIBUTE_CREATED.notificationTrigger,
                ATTRIBUTE_UPDATED.notificationTrigger,
                ATTRIBUTE_DELETED.notificationTrigger
            ).joinToString(",")
    }
}

fun List<Subscription>.prepareForRendering(
    contexts: List<String>,
    mediaType: MediaType = JSON_LD_MEDIA_TYPE,
    includeSysAttrs: Boolean = false
): String =
    this.map {
        convertTo<Map<String, Any>>(it.compact(contexts))
            .toFinalRepresentation(mediaType, includeSysAttrs)
    }.let {
        serialize(it)
    }

fun List<Subscription>.mergeEntitySelectorsOnSubscriptions() =
    this.groupBy { t: Subscription -> t.id }
        .mapValues { grouped ->
            grouped.value.reduce { t: Subscription, u: Subscription ->
                t.copy(entities = mergeEntitySelectors(t.entities, u.entities))
            }
        }.values.toList()

private fun mergeEntitySelectors(
    target: Set<EntitySelector>?,
    source: Set<EntitySelector>?
): Set<EntitySelector>? =
    if (target == null) source
    else if (source == null) target
    else target.plus(source)
