package com.egm.stellio.search.service

import arrow.core.*
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.addContextToElement
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerms
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.extractAttributeInstanceFromCompactedEntity
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.shared.util.toUri
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.net.URI
import java.time.ZonedDateTime

@Component
class EntityEventListenerService(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val entityPayloadService: EntityPayloadService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["cim.entity._CatchAll"], groupId = "context_search")
    fun processMessage(content: String) {
        logger.debug("Processing message: $content")
        when (val entityEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleEntityCreateEvent(entityEvent)
            is EntityReplaceEvent -> handleEntityReplaceEvent(entityEvent)
            is EntityDeleteEvent -> handleEntityDeleteEvent(entityEvent)
            is AttributeAppendEvent ->
                when (entityEvent.attributeName) {
                    JSONLD_TYPE_TERM -> handleEntityTypeAppendEvent(entityEvent)
                    else -> handleAttributeAppendEvent(entityEvent)
                }
            is AttributeReplaceEvent -> handleAttributeReplaceEvent(entityEvent)
            is AttributeUpdateEvent -> handleAttributeUpdateEvent(entityEvent)
            is AttributeDeleteEvent -> handleAttributeDeleteEvent(entityEvent)
            is AttributeDeleteAllInstancesEvent -> handleAttributeDeleteAllInstancesEvent(entityEvent)
        }
    }

    private fun handleEntityCreateEvent(entityCreateEvent: EntityCreateEvent) {
        val operationPayload = addContextToElement(entityCreateEvent.operationPayload, entityCreateEvent.contexts)
        temporalEntityAttributeService.createEntityTemporalReferences(
            operationPayload,
            entityCreateEvent.contexts,
            entityCreateEvent.sub
        ).then(
            entityAccessRightsService.setAdminRoleOnEntity(
                entityCreateEvent.sub,
                entityCreateEvent.entityId
            )
        ).subscribe(
            { logger.debug("Created temporal entity ${entityCreateEvent.entityId}") },
            {
                logger.warn("Failed to create temporal entity ${entityCreateEvent.entityId}: ${it.message}")
            }
        )
    }

    private fun handleEntityReplaceEvent(entityReplaceEvent: EntityReplaceEvent) {
        val operationPayload = addContextToElement(entityReplaceEvent.operationPayload, entityReplaceEvent.contexts)
        temporalEntityAttributeService.deleteTemporalEntityReferences(entityReplaceEvent.entityId)
            .then(
                temporalEntityAttributeService.createEntityTemporalReferences(
                    operationPayload,
                    entityReplaceEvent.contexts,
                    entityReplaceEvent.sub
                )
            ).then(
                entityAccessRightsService.setAdminRoleOnEntity(
                    entityReplaceEvent.sub,
                    entityReplaceEvent.entityId
                )
            ).subscribe(
                { logger.debug("Replaced entity ${entityReplaceEvent.entityId} (records replaced: $it)") },
                {
                    logger.warn("Failed to replace temporal entity ${entityReplaceEvent.entityId}: ${it.message}")
                }

            )
    }

    private fun handleEntityDeleteEvent(entityDeleteEvent: EntityDeleteEvent) =
        temporalEntityAttributeService.deleteTemporalEntityReferences(entityDeleteEvent.entityId)
            .then(
                entityAccessRightsService.removeRolesOnEntity(entityDeleteEvent.entityId)
            )
            .subscribe(
                { logger.debug("Deleted entity ${entityDeleteEvent.entityId} (records deleted: $it)") },
                {
                    logger.warn("Failed to delete temporal entity ${entityDeleteEvent.entityId}: ${it.message}")
                }
            )

    private fun handleAttributeDeleteEvent(attributeDeleteEvent: AttributeDeleteEvent) {
        val expandedAttributeName =
            expandJsonLdTerm(attributeDeleteEvent.attributeName, attributeDeleteEvent.contexts)
        val compactedJsonLdEntity = addContextsToEntity(
            deserializeObject(attributeDeleteEvent.updatedEntity),
            attributeDeleteEvent.contexts
        )

        temporalEntityAttributeService.deleteTemporalAttributeReferences(
            attributeDeleteEvent.entityId,
            expandedAttributeName,
            attributeDeleteEvent.datasetId
        ).then(
            entityPayloadService.upsertEntityPayload(
                attributeDeleteEvent.entityId,
                serializeObject(compactedJsonLdEntity)
            )
        ).subscribe(
            {
                logger.debug(
                    "Deleted temporal attribute $expandedAttributeName with datasetId " +
                        "${attributeDeleteEvent.datasetId} from entity ${attributeDeleteEvent.entityId} "
                )
            },
            {
                logger.warn(
                    "Failed to delete temporal attribute $expandedAttributeName from entity " +
                        "${attributeDeleteEvent.entityId}: ${it.message}"
                )
            }

        )
    }

    private fun handleAttributeDeleteAllInstancesEvent(
        attributeDeleteAllInstancesEvent: AttributeDeleteAllInstancesEvent
    ) {
        val expandedAttributeName = expandJsonLdTerm(
            attributeDeleteAllInstancesEvent.attributeName, attributeDeleteAllInstancesEvent.contexts
        )
        val compactedJsonLdEntity = addContextsToEntity(
            deserializeObject(attributeDeleteAllInstancesEvent.updatedEntity),
            attributeDeleteAllInstancesEvent.contexts
        )

        temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(
            attributeDeleteAllInstancesEvent.entityId,
            expandedAttributeName
        ).then(
            entityPayloadService.upsertEntityPayload(
                attributeDeleteAllInstancesEvent.entityId,
                serializeObject(compactedJsonLdEntity)
            )
        ).subscribe(
            {
                logger.debug(
                    "Deleted all temporal attributes of $expandedAttributeName " +
                        "from entity ${attributeDeleteAllInstancesEvent.entityId}"
                )
            },
            {
                logger.warn(
                    "Failed to delete all temporal attributes of $expandedAttributeName from entity " +
                        "${attributeDeleteAllInstancesEvent.entityId}: ${it.message}"
                )
            }

        )
    }

    private fun handleEntityTypeAppendEvent(attributeAppendEvent: AttributeAppendEvent) {
        val (_, entityId, entityTypes, _, _, _, _, _, contexts) = attributeAppendEvent
        temporalEntityAttributeService.updateTemporalEntityTypes(entityId, expandJsonLdTerms(entityTypes, contexts))
            .subscribe(
                {
                    logger.debug("Updated types of entity $entityId to $entityTypes")
                },
                {
                    logger.warn("Failed to update entity types of entity $entityId: ${it.message}")
                }
            )
    }

    private fun handleAttributeAppendEvent(attributeAppendEvent: AttributeAppendEvent) {
        handleAttributeAppend(attributeAppendEvent)
    }

    private fun handleAttributeReplaceEvent(attributeReplaceEvent: AttributeReplaceEvent) {
        val operationPayloadNode = mapper.readTree(attributeReplaceEvent.operationPayload)

        // FIXME since the NGSI-LD API still misses a proper API to partially update a list of attributes,
        //  replace events with an observedAt property are considered as observation updates, and not as audit ones
        handleAttributeUpdate(
            attributeReplaceEvent.sub,
            attributeReplaceEvent.entityId,
            attributeReplaceEvent.entityTypes,
            attributeReplaceEvent.attributeName,
            attributeReplaceEvent.datasetId,
            operationPayloadNode.has("observedAt"),
            attributeReplaceEvent.updatedEntity,
            attributeReplaceEvent.contexts
        )
    }

    private fun handleAttributeUpdateEvent(attributeUpdateEvent: AttributeUpdateEvent) {
        val operationPayloadNode = mapper.readTree(attributeUpdateEvent.operationPayload)

        handleAttributeUpdate(
            attributeUpdateEvent.sub,
            attributeUpdateEvent.entityId,
            attributeUpdateEvent.entityTypes,
            attributeUpdateEvent.attributeName,
            attributeUpdateEvent.datasetId,
            operationPayloadNode.has("observedAt"),
            attributeUpdateEvent.updatedEntity,
            attributeUpdateEvent.contexts
        )
    }

    private fun handleAttributeUpdate(
        sub: String?,
        entityId: URI,
        entityTypes: List<String>,
        attributeName: String,
        datasetId: URI?,
        isNewObservation: Boolean,
        updatedEntity: String,
        contexts: List<String>
    ) {
        val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)
        val fullAttributeInstance = extractAttributeInstanceFromCompactedEntity(
            compactedJsonLdEntity,
            attributeName,
            datasetId
        )

        when (val extractedAttributeMetadata = toTemporalAttributeMetadata(fullAttributeInstance)) {
            is Invalid -> {
                logger.info(extractedAttributeMetadata.value)
                return
            }
            is Valid -> {
                val attributeMetadata = extractedAttributeMetadata.value
                val expandedAttributeName = expandJsonLdTerm(attributeName, contexts)
                temporalEntityAttributeService.getForEntityAndAttribute(
                    entityId, expandedAttributeName, attributeMetadata.datasetId
                ).switchIfEmpty {
                    // in case of an existing attribute not handled previously by search service (not using observedAt)
                    // we transparently create the temporal entity attribute
                    val temporalEntityAttribute = TemporalEntityAttribute(
                        entityId = entityId,
                        types = expandJsonLdTerms(entityTypes, contexts),
                        attributeName = expandedAttributeName,
                        attributeType = attributeMetadata.type,
                        attributeValueType = attributeMetadata.valueType,
                        datasetId = attributeMetadata.datasetId
                    )
                    temporalEntityAttributeService.create(temporalEntityAttribute)
                        .map { temporalEntityAttribute.id }
                }.flatMap {
                    val timeAndProperty =
                        if (isNewObservation)
                            Pair(attributeMetadata.observedAt!!, TemporalProperty.OBSERVED_AT)
                        else if (attributeMetadata.modifiedAt != null)
                            Pair(attributeMetadata.modifiedAt, TemporalProperty.MODIFIED_AT)
                        // in case of an attribute replace
                        else Pair(attributeMetadata.createdAt, TemporalProperty.MODIFIED_AT)
                    val attributeInstance = AttributeInstance(
                        temporalEntityAttribute = it,
                        timeProperty = timeAndProperty.second,
                        time = timeAndProperty.first,
                        value = attributeMetadata.value,
                        measuredValue = attributeMetadata.measuredValue,
                        payload = fullAttributeInstance,
                        sub = sub
                    )
                    attributeInstanceService.create(attributeInstance)
                }.flatMap {
                    entityPayloadService.upsertEntityPayload(entityId, serializeObject(compactedJsonLdEntity))
                }.subscribe(
                    { logger.debug("Created new attribute instance of $expandedAttributeName for $entityId") },
                    {
                        logger.warn(
                            "Failed to persist new attribute instance $expandedAttributeName " +
                                "for $entityId, ignoring it (${it.message})"
                        )
                    }
                )
            }
        }
    }

    private fun handleAttributeAppend(attributeAppendEvent: AttributeAppendEvent) {
        val (sub, entityId, entityTypes, attributeName, datasetId, _, _, updatedEntity, contexts) = attributeAppendEvent
        val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)
        val fullAttributeInstance = extractAttributeInstanceFromCompactedEntity(
            compactedJsonLdEntity,
            attributeName,
            datasetId
        )

        when (val extractedAttributeMetadata = toTemporalAttributeMetadata(fullAttributeInstance)) {
            is Invalid -> {
                logger.info(extractedAttributeMetadata.value)
                return
            }
            is Valid -> {
                val attributeMetadata = extractedAttributeMetadata.value
                val expandedAttributeName = expandJsonLdTerm(attributeName, contexts)

                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = entityId,
                    types = expandJsonLdTerms(entityTypes, contexts),
                    attributeName = expandedAttributeName,
                    attributeType = attributeMetadata.type,
                    attributeValueType = attributeMetadata.valueType,
                    datasetId = attributeMetadata.datasetId
                )
                val attributeInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    timeProperty = TemporalProperty.CREATED_AT,
                    time = attributeMetadata.createdAt,
                    measuredValue = attributeMetadata.measuredValue,
                    value = attributeMetadata.value,
                    payload = fullAttributeInstance,
                    sub = sub
                )

                val attributeObservedAtInstanceMono =
                    if (attributeMetadata.observedAt != null) {
                        val attributeObservedAtInstance = attributeInstance.copy(
                            time = attributeMetadata.observedAt,
                            timeProperty = TemporalProperty.OBSERVED_AT
                        )
                        attributeInstanceService.create(attributeObservedAtInstance)
                    } else Mono.just(1)

                temporalEntityAttributeService.create(temporalEntityAttribute)
                    .then(attributeInstanceService.create(attributeInstance))
                    .then(attributeObservedAtInstanceMono)
                    .then(entityPayloadService.upsertEntityPayload(entityId, serializeObject(compactedJsonLdEntity)))
                    .subscribe(
                        {
                            logger.debug("Created temporal entity attribute $expandedAttributeName for $entityId ")
                        },
                        {
                            logger.error(
                                "Failed to persist new temporal entity attribute for $entityId " +
                                    "with attribute instance $expandedAttributeName, ignoring it (${it.message})"
                            )
                        }
                    )
            }
        }
    }

    internal fun toTemporalAttributeMetadata(
        jsonLdEntity: CompactedJsonLdEntity
    ): Validated<String, AttributeMetadata> {
        val attributeTypeAsText = jsonLdEntity["type"] as String
        val attributeType = kotlin.runCatching {
            TemporalEntityAttribute.AttributeType.valueOf(attributeTypeAsText)
        }.getOrNull() ?: return "Unsupported attribute type: $attributeTypeAsText".invalid()
        val attributeValue = when (attributeType) {
            TemporalEntityAttribute.AttributeType.Relationship -> Pair(jsonLdEntity["object"] as String, null)
            TemporalEntityAttribute.AttributeType.Property ->
                when (val rawAttributeValue = jsonLdEntity["value"]) {
                    null ->
                        return "Unable to get a value from attribute: $jsonLdEntity".invalid()
                    else -> Pair(valueToStringOrNull(rawAttributeValue), valueToDoubleOrNull(rawAttributeValue))
                }
        }
        val attributeValueType =
            if (attributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
            else TemporalEntityAttribute.AttributeValueType.ANY

        val observedAt =
            if (jsonLdEntity["observedAt"] != null)
                ZonedDateTime.parse(jsonLdEntity["observedAt"] as String)
            else null

        val modifiedAt =
            if (jsonLdEntity["modifiedAt"] != null)
                ZonedDateTime.parse(jsonLdEntity["modifiedAt"] as String)
            else null

        return AttributeMetadata(
            measuredValue = attributeValue.second,
            value = attributeValue.first,
            valueType = attributeValueType,
            datasetId = (jsonLdEntity["datasetId"] as? String)?.toUri(),
            type = attributeType,
            createdAt = ZonedDateTime.parse(jsonLdEntity["createdAt"] as String),
            modifiedAt = modifiedAt,
            observedAt = observedAt
        ).valid()
    }
}
