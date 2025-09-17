package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AttributeRepresentation
import com.egm.stellio.shared.model.AttributesValuesMapping.Companion.fromAttributeNameTerm
import com.egm.stellio.shared.model.COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.EntityRepresentation
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_NULL
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.NgsiLdDataRepresentation
import com.egm.stellio.shared.model.applyAttributeTransformation
import com.egm.stellio.shared.model.filterPickAndOmit
import com.egm.stellio.shared.model.getTypeAndValue
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.util.JsonLdUtils.compactAttribute
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.acceptToMediaType
import com.egm.stellio.shared.util.getTenantFromContext
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import com.egm.stellio.subscription.model.NotificationParams.JoinType
import com.egm.stellio.subscription.model.NotificationTrigger
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.mqtt.MqttNotificationService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

@Service
class NotificationService(
    private val subscriptionService: SubscriptionService,
    private val coreAPIService: CoreAPIService,
    private val mqttNotificationService: MqttNotificationService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun notifyMatchingSubscribers(
        tenantName: String,
        updatedAttribute: Pair<ExpandedTerm, URI?>?,
        previousPayload: Map<String, List<Any>>?,
        expandedEntity: ExpandedEntity,
        notificationTrigger: NotificationTrigger
    ): Either<APIException, List<Triple<Subscription, Notification, Boolean>>> = either {
        subscriptionService.getMatchingSubscriptions(
            expandedEntity,
            updatedAttribute,
            notificationTrigger
        ).bind().map {
            val contexts = it.jsonldContext?.let { listOf(it.toString()) } ?: it.contexts
            val compactedEntities =
                if (it.notification.join in listOf(JoinType.FLAT, JoinType.INLINE)) {
                    coreAPIService.retrieveLinkedEntities(
                        tenantName,
                        expandedEntity.id,
                        it.notification,
                        subscriptionService.getContextsLink(it)
                    )
                } else {
                    val filteredEntity = expandedEntity.filterAttributes(
                        it.notification.attributes?.toSet().orEmpty()
                    ).applyDatasetView(
                        it.datasetId?.toSet().orEmpty()
                    )
                    val entityRepresentation =
                        EntityRepresentation.forMediaType(acceptToMediaType(it.notification.endpoint.accept.accept))
                    val attributeRepresentation =
                        if (it.notification.format in listOf(FormatType.KEY_VALUES, FormatType.SIMPLIFIED))
                            AttributeRepresentation.SIMPLIFIED
                        else AttributeRepresentation.NORMALIZED

                    compactEntity(
                        filteredEntity,
                        contexts
                    ).filterPickAndOmit(
                        it.notification.pick.orEmpty(),
                        it.notification.omit.orEmpty()
                    ).toFinalRepresentation(
                        NgsiLdDataRepresentation(
                            entityRepresentation,
                            attributeRepresentation,
                            it.notification.sysAttrs,
                            it.lang
                        )
                    ).let { listOf(it) }
                }

            val compactedEntitiesWithPreviousValues =
                if (it.notification.showChanges)
                    addChangesInNotifiedEntity(
                        expandedEntity.id,
                        compactedEntities,
                        updatedAttribute,
                        previousPayload,
                        notificationTrigger,
                        contexts
                    )
                else compactedEntities

            callSubscriber(it, compactedEntitiesWithPreviousValues)
        }
    }

    internal fun addChangesInNotifiedEntity(
        entityId: URI,
        compactedEntities: List<Map<String, Any?>>,
        updatedAttribute: Pair<ExpandedTerm, URI?>?,
        previousPayload: Map<String, List<Any>>?,
        notificationTrigger: NotificationTrigger,
        contexts: List<String>
    ): List<Map<String, Any?>> {
        // since the notification can contain linked entities, extract the "main" entity from the list
        val notifiedEntity = compactedEntities.first { it[NGSILD_ID_TERM] == entityId.toString() } as CompactedEntity

        val notifiedEntityWithPreviousValue = when (notificationTrigger) {
            NotificationTrigger.ATTRIBUTE_UPDATED, NotificationTrigger.ATTRIBUTE_DELETED -> {
                updateWithPreviousAttributeValue(updatedAttribute!!, previousPayload!!, contexts, notifiedEntity)
            }
            NotificationTrigger.ENTITY_DELETED -> {
                addPreviousAttributesValues(previousPayload!!, contexts, notifiedEntity)
            }
            else -> notifiedEntity
        }

        return compactedEntities
            .filter { it[NGSILD_ID_TERM] != entityId.toString() }
            .plus(notifiedEntityWithPreviousValue)
    }

    private fun addPreviousAttributesValues(
        previousPayload: Map<String, List<Any>>,
        contexts: List<String>,
        notifiedEntity: CompactedEntity
    ): Map<String, Any> {
        val compactedPreviousEntity = compactEntity(ExpandedEntity(previousPayload), contexts)
        val previousAttributesValues = compactedPreviousEntity.minus(COMPACTED_ENTITY_CORE_MEMBERS)
            .mapValues { entry ->
                applyAttributeTransformation(
                    entry,
                    { value -> addPreviousAttributeInstance(value) },
                    { values ->
                        values.map { instanceValue -> addPreviousAttributeInstance(instanceValue) }
                    }
                )
            }

        return notifiedEntity.plus(previousAttributesValues)
    }

    private fun addPreviousAttributeInstance(instanceValue: Map<String, Any>): Map<String, Any?> {
        val compactedAttributeTypeAndValue = instanceValue.getTypeAndValue()
        val attributeValueMappings = fromAttributeNameTerm(compactedAttributeTypeAndValue.first)
        return mapOf(
            NGSILD_TYPE_TERM to compactedAttributeTypeAndValue.first,
            attributeValueMappings.previousValueTerm to compactedAttributeTypeAndValue.second,
            attributeValueMappings.valueTerm to NGSILD_NULL
        ).let {
            if (instanceValue[NGSILD_DATASET_ID_TERM] != null)
                it.plus(NGSILD_DATASET_ID_TERM to instanceValue[NGSILD_DATASET_ID_TERM])
            else it
        }
    }

    private fun updateWithPreviousAttributeValue(
        updatedAttribute: Pair<ExpandedTerm, URI?>,
        previousPayload: Map<String, List<Any>>,
        contexts: List<String>,
        notifiedEntity: CompactedEntity
    ): Map<String, Any> {
        val computedPreviousAttributeAndFragment = computePreviousAttributeAndFragment(
            updatedAttribute,
            previousPayload,
            contexts
        )
        val inject = { value: Map<String, Any>, datasetId: String? ->
            if (value[NGSILD_DATASET_ID_TERM] == datasetId) {
                value.plus(computedPreviousAttributeAndFragment.second)
            } else value
        }

        return notifiedEntity.mapValues { entry ->
            if (entry.key == computedPreviousAttributeAndFragment.first) {
                val updatedAttributeDatasetId = updatedAttribute.second?.toString()
                applyAttributeTransformation(
                    entry,
                    { value -> inject(value, updatedAttributeDatasetId) },
                    { values -> values.map { instanceValue -> inject(instanceValue, updatedAttributeDatasetId) } }
                )
            } else entry.value
        }
    }

    private fun computePreviousAttributeAndFragment(
        updatedAttribute: Pair<ExpandedTerm, URI?>,
        previousPayload: ExpandedAttributeInstance,
        contexts: List<String>
    ): Pair<String, Map<String, Any?>> {
        val expandedPreviousMember = mapOf(updatedAttribute.first to listOf(previousPayload))
        val compactedPreviousMember = compactAttribute(expandedPreviousMember, contexts)
        val compactedAttributeName = compactedPreviousMember.keys.first()
        val compactedAttributeTypeAndValue = compactedPreviousMember[compactedAttributeName]!!.getTypeAndValue()
        val attributeValueMappings = fromAttributeNameTerm(compactedAttributeTypeAndValue.first)

        return compactedAttributeName to mapOf(
            attributeValueMappings.previousValueTerm to compactedAttributeTypeAndValue.second
        )
    }

    suspend fun callSubscriber(
        subscription: Subscription,
        entities: List<Map<String, Any?>>
    ): Triple<Subscription, Notification, Boolean> {
        val mediaType = MediaType.valueOf(subscription.notification.endpoint.accept.accept)
        val tenantName = getTenantFromContext()
        val notification = Notification(
            subscriptionId = subscription.id,
            data = entities
        )
        val uri = subscription.notification.endpoint.uri.toString()
        logger.info("Notification is about to be sent to $uri for subscription ${subscription.id}")

        val headerMap: MutableMap<String, String> = emptyMap<String, String>().toMutableMap()
        if (mediaType == MediaType.APPLICATION_JSON) {
            headerMap[HttpHeaders.LINK] = subscriptionService.getContextsLink(subscription)
        }
        if (tenantName != DEFAULT_TENANT_NAME)
            headerMap[NGSILD_TENANT_HEADER] = tenantName
        subscription.notification.endpoint.receiverInfo?.forEach { endpointInfo ->
            headerMap[endpointInfo.key] = endpointInfo.value
        }
        headerMap[HttpHeaders.CONTENT_TYPE] = mediaType.toString()

        val result =
            kotlin.runCatching {
                if (uri.startsWith(Endpoint.MQTT_SCHEME)) {
                    Triple(
                        subscription,
                        notification,
                        mqttNotificationService.notify(
                            notification = notification,
                            subscription = subscription,
                            headers = headerMap
                        )
                    )
                } else {
                    val request =
                        WebClient.create(uri).post().headers { it.setAll(headerMap) }
                    request
                        .bodyValue(serializeObject(notification))
                        .awaitExchange { response ->
                            val success = response.statusCode() == HttpStatus.OK
                            logger.info(
                                "The notification sent has been received with ${if (success) "success" else "failure"}"
                            )
                            if (!success) {
                                logger.error("Failed to send notification to $uri: ${response.statusCode()}")
                            }
                            Triple(subscription, notification, success)
                        }
                }
            }.getOrElse {
                Triple(subscription, notification, false)
            }

        subscriptionService.updateSubscriptionNotification(result.first, result.second, result.third)
        return result
    }
}
