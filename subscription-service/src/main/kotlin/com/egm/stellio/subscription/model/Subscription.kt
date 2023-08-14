package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.ExpandedTerm
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SYSATTRS_TERMS
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.convertToMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.http.MediaType
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

data class Subscription(
    @Id val id: URI = "urn:ngsi-ld:Subscription:${UUID.randomUUID()}".toUri(),
    val type: String,
    val subscriptionName: String? = null,
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),
    val modifiedAt: ZonedDateTime? = null,
    val description: String? = null,
    val entities: Set<EntityInfo>,
    val watchedAttributes: List<ExpandedTerm>? = null,
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
    val contexts: List<ExpandedTerm>
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
            entities = entities.map { entityInfo ->
                entityInfo.copy(
                    type = expandJsonLdTerm(entityInfo.type, contexts)
                )
            }.toSet(),
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
            entities = entities.map {
                EntityInfo(it.id, it.idPattern, compactTerm(it.type, contexts))
            }.toSet(),
            notification = notification.copy(
                attributes = notification.attributes?.map { compactTerm(it, contexts) }
            ),
            geoQ = geoQ?.copy(
                geoproperty = compactTerm(geoQ.geoproperty, contexts)
            ),
            watchedAttributes = this.watchedAttributes?.map { compactTerm(it, contexts) }
        )

    fun compact(context: String): Subscription =
        compact(listOf(context))

    fun serialize(
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String =
        convertToMap(this.compact(contexts))
            .toFinalRepresentation(mediaType, includeSysAttrs)
            .let { serializeObject(it) }

    fun serialize(
        context: String,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String =
        serialize(listOf(context), mediaType, includeSysAttrs)
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

fun Map<String, Any>.toFinalRepresentation(
    mediaType: MediaType = JSON_LD_MEDIA_TYPE,
    includeSysAttrs: Boolean = false
): Map<String, Any> =
    this.let {
        if (mediaType == MediaType.APPLICATION_JSON)
            it.minus(JSONLD_CONTEXT)
        else it
    }.let {
        if (!includeSysAttrs)
            it.minus(NGSILD_SYSATTRS_TERMS)
        else it
    }

fun List<Subscription>.serialize(
    context: String,
    mediaType: MediaType = JSON_LD_MEDIA_TYPE,
    includeSysAttrs: Boolean = false
): String =
    this.map {
        convertToMap(it.compact(context))
            .toFinalRepresentation(mediaType, includeSysAttrs)
    }.let {
        serializeObject(it)
    }
