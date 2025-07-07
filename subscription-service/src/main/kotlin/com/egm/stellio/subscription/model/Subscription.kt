package com.egm.stellio.subscription.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.DataTypes.convertTo
import com.egm.stellio.shared.util.DataTypes.deserializeAs
import com.egm.stellio.shared.util.DataTypes.serialize
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.checkJsonldContext
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.compactTypeSelection
import com.egm.stellio.shared.util.expandTypeSelection
import com.egm.stellio.shared.util.invalidUriMessage
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toFinalRepresentation
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.NotificationParams.JoinType
import com.egm.stellio.subscription.model.NotificationTrigger.ATTRIBUTE_CREATED
import com.egm.stellio.subscription.model.NotificationTrigger.ATTRIBUTE_UPDATED
import com.egm.stellio.subscription.service.mqtt.Mqtt.SCHEME.MQTT
import com.egm.stellio.subscription.service.mqtt.Mqtt.SCHEME.MQTTS
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.http.MediaType
import java.net.URI
import java.time.ZonedDateTime
import java.util.*
import java.util.regex.Pattern

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
    @JsonProperty(value = JSONLD_CONTEXT_KW)
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

    fun validate(): Either<APIException, Subscription> = either {
        checkTypeIsSubscription().bind()
        checkIdIsValid().bind()
        checkEntitiesOrWatchedAttributes().bind()
        checkTimeIntervalGreaterThanZero().bind()
        checkThrottlingGreaterThanZero().bind()
        checkSubscriptionValidity().bind()
        checkExpiresAtInTheFuture().bind()
        checkIdPatternIsValid().bind()
        checkNotificationTriggersAreValid().bind()
        checkJsonLdContextIsValid().bind()
        checkJoinParametersAreValid().bind()
        checkEndpointUriIsValid().bind()

        this@Subscription
    }

    private fun checkTypeIsSubscription(): Either<APIException, Unit> =
        if (type != NGSILD_SUBSCRIPTION_TERM)
            BadRequestDataException("type attribute must be equal to 'Subscription'").left()
        else Unit.right()

    private fun checkIdIsValid(): Either<APIException, Unit> =
        if (!id.isAbsolute)
            BadRequestDataException(invalidUriMessage("$id")).left()
        else Unit.right()

    private fun checkEntitiesOrWatchedAttributes(): Either<APIException, Unit> =
        if (watchedAttributes == null && entities == null)
            BadRequestDataException("At least one of entities or watchedAttributes shall be present").left()
        else Unit.right()

    private fun checkSubscriptionValidity(): Either<APIException, Unit> =
        when {
            watchedAttributes != null && timeInterval != null -> {
                BadRequestDataException(
                    "You can't use 'timeInterval' in conjunction with 'watchedAttributes'"
                ).left()
            }

            timeInterval != null && throttling != null -> {
                BadRequestDataException(
                    "You can't use 'timeInterval' in conjunction with 'throttling'"
                ).left()
            }

            else ->
                Unit.right()
        }

    private fun checkTimeIntervalGreaterThanZero(): Either<APIException, Unit> =
        if (timeInterval != null && timeInterval < 1)
            BadRequestDataException("The value of 'timeInterval' must be greater than zero (int)").left()
        else Unit.right()

    private fun checkThrottlingGreaterThanZero(): Either<APIException, Unit> =
        if (throttling != null && throttling < 1)
            BadRequestDataException("The value of 'throttling' must be greater than zero (int)").left()
        else Unit.right()

    private fun checkExpiresAtInTheFuture(): Either<BadRequestDataException, Unit> =
        if (expiresAt != null && expiresAt.isBefore(ngsiLdDateTime()))
            BadRequestDataException("'expiresAt' must be in the future").left()
        else Unit.right()

    private fun checkIdPatternIsValid(): Either<BadRequestDataException, Unit> {
        val result = entities?.all { endpoint ->
            runCatching {
                endpoint.idPattern?.let { Pattern.compile(it) }
                true
            }.fold({ true }, { false })
        }

        return if (result == null || result)
            Unit.right()
        else BadRequestDataException("Invalid idPattern found in subscription").left()
    }

    private fun checkNotificationTriggersAreValid(): Either<BadRequestDataException, Unit> =
        notificationTrigger.all {
            NotificationTrigger.isValid(it)
        }.let {
            if (it) Unit.right()
            else BadRequestDataException("Unknown notification trigger in $notificationTrigger").left()
        }

    private fun checkJsonLdContextIsValid(): Either<APIException, Unit> = either {
        if (jsonldContext != null) {
            checkJsonldContext(jsonldContext).bind()
        }
    }

    private fun checkJoinParametersAreValid(): Either<BadRequestDataException, Unit> {
        if (notification.join != null && notification.join != JoinType.NONE) {
            notification.joinLevel?.let {
                if (it < 1)
                    return BadRequestDataException(
                        "The value of 'joinLevel' must be greater than zero (int) if 'join' is asked"
                    ).left()
            }
        }

        return Unit.right()
    }

    private fun checkEndpointUriIsValid(): Either<BadRequestDataException, Unit> {
        if (notification.endpoint.uri.scheme !in validEndpointUriSchemes)
            return BadRequestDataException("Invalid URI for endpoint: ${notification.endpoint.uri}").left()
        return Unit.right()
    }

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

    fun mergeWithFragment(
        fragment: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, Subscription> = either {
        val mergedSubscription = convertTo<Map<String, Any>>(this@Subscription).plus(fragment)
        deserialize(mergedSubscription, contexts).bind()
            .copy(modifiedAt = ngsiLdDateTime())
    }

    fun mergeWithFragment(fragment: String, contexts: List<String>): Either<APIException, Subscription> =
        mergeWithFragment(fragment.deserializeAsMap(), contexts)

    companion object {

        val notImplementedAttributes: List<String> = listOf("csf", "temporalQ")
        val validEndpointUriSchemes: List<String> = listOf("http", "https", MQTT, MQTTS)

        fun deserialize(input: Map<String, Any>, contexts: List<String>): Either<APIException, Subscription> =
            runCatching {
                deserializeAs<Subscription>(serializeObject(input.plus(JSONLD_CONTEXT_KW to contexts)))
                    .expand(contexts)
            }.fold(
                { it.right() },
                {
                    if (it is UnrecognizedPropertyException) {
                        if (it.propertyName in notImplementedAttributes)
                            NotImplementedException(
                                "Attribute ${it.propertyName} is not yet implemented in subscriptions"
                            ).left()
                        else
                            BadRequestDataException("Invalid attribute ${it.propertyName} in subscription").left()
                    } else it.toAPIException("Failed to parse subscription: ${it.message}").left()
                }
            )
    }
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
