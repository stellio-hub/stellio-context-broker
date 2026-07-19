package com.egm.stellio.search.csr.util

import arrow.core.Either
import arrow.core.Ior
import arrow.core.IorNel
import arrow.core.left
import arrow.core.raise.Raise
import arrow.core.raise.either
import arrow.core.right
import arrow.core.toNonEmptyListOrNull
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedAttributeInstances
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.model.NGSILD_EXPIRES_AT_TERM
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.model.NGSILD_SYSATTRS_TERMS
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.TEMPORAL_REPRESENTATION_TERMS
import com.egm.stellio.shared.model.applyAttributeTransformation
import com.egm.stellio.shared.util.ErrorMessages.Csr.contextSourceInvalidPayloadMessage
import com.egm.stellio.shared.util.isDateTime
import com.egm.stellio.shared.util.ngsiLdDateTime
import java.time.ZonedDateTime

typealias CompactedEntityWithCSR = Pair<CompactedEntity, ContextSourceRegistration>
typealias CompactedEntitiesWithCSR = Pair<List<CompactedEntity>, ContextSourceRegistration>

typealias AttributeByDatasetId = Map<String?, CompactedAttributeInstance>

object ContextSourceUtils {

    /**
     * Implements 4.5.5.2 - Processing of Conflicting Transient Entities: pushes an entity-level expiresAt down onto
     * every attribute of the entity that has no expiresAt of its own, or that has a later one than the entity's.
     */
    fun propagateExpiresAtToAttributes(entity: CompactedEntity): CompactedEntity {
        val entityExpiresAt = entity[NGSILD_EXPIRES_AT_TERM] as? String ?: return entity
        return entity.mapValues { entry ->
            applyAttributeTransformation(
                entry,
                { propagateExpiresAtToAttributeInstance(it, entityExpiresAt) },
                { instances -> instances.map { propagateExpiresAtToAttributeInstance(it, entityExpiresAt) } }
            )
        }
    }

    private fun propagateExpiresAtToAttributeInstance(
        instance: CompactedAttributeInstance,
        entityExpiresAt: String
    ): CompactedAttributeInstance {
        val attributeExpiresAt = instance[NGSILD_EXPIRES_AT_TERM] as? String
        return when {
            attributeExpiresAt == null -> instance.plus(NGSILD_EXPIRES_AT_TERM to entityExpiresAt)
            entityExpiresAt.isBefore(attributeExpiresAt) -> instance.plus(NGSILD_EXPIRES_AT_TERM to entityExpiresAt)
            else -> instance
        }
    }

    /**
     * Implements 4.5.5.3 - Processing of Conflicting Attributes (entity-level expiresAt): if expiresAt is missing
     * from at least one of the sources contributing to this entity, it is dropped entirely; otherwise the furthest
     * in the future value is kept.
     */
    private fun resolveEntityExpiresAtConflict(
        mergedEntity: MutableMap<String, Any>,
        sourceEntities: List<CompactedEntity>
    ) {
        val expiresAtValues = sourceEntities.map { it[NGSILD_EXPIRES_AT_TERM] as? String }
        if (expiresAtValues.isEmpty()) return
        if (expiresAtValues.any { it == null })
            mergedEntity.remove(NGSILD_EXPIRES_AT_TERM)
        else
            mergedEntity[NGSILD_EXPIRES_AT_TERM] = expiresAtValues.filterNotNull()
                .maxWith { a, b -> ZonedDateTime.parse(a).compareTo(ZonedDateTime.parse(b)) }
    }

    fun mergeEntitiesLists(
        localEntities: List<CompactedEntity>,
        remoteEntitiesWithCSR: List<CompactedEntitiesWithCSR>
    ): IorNel<NGSILDWarning, List<CompactedEntity>> {
        val mergedEntityMap = localEntities.map { it.toMutableMap() }.associateBy { it[NGSILD_ID_TERM] }.toMutableMap()
        val remoteEntitiesWithExpiresAtPropagated = remoteEntitiesWithCSR.map { (entities, csr) ->
            Pair(entities.map { propagateExpiresAtToAttributes(it) }, csr)
        }

        val warnings = remoteEntitiesWithExpiresAtPropagated.sortedBy { (_, csr) -> csr.isAuxiliary() }
            .mapNotNull { (entities, csr) ->
                either {
                    entities.forEach { entity ->
                        val id = entity[NGSILD_ID_TERM]
                        mergedEntityMap[id]
                            ?.let { it.putAll(getMergeNewValues(it, entity, csr).bind()) }
                            ?: run { mergedEntityMap[id] = entity.toMutableMap() }
                    }
                    null
                }.leftOrNull()
            }.toNonEmptyListOrNull()

        val sourceEntitiesById = (localEntities + remoteEntitiesWithExpiresAtPropagated.flatMap { it.first })
            .groupBy { it[NGSILD_ID_TERM] }
        mergedEntityMap.forEach { (id, mergedEntity) ->
            resolveEntityExpiresAtConflict(mergedEntity, sourceEntitiesById[id] ?: emptyList())
        }

        val entities = mergedEntityMap.values.toList()
        return if (warnings == null) Ior.Right(entities) else Ior.Both(warnings, entities)
    }

    fun mergeEntities(
        localEntity: CompactedEntity?,
        remoteEntitiesWithCSR: List<CompactedEntityWithCSR>
    ): IorNel<NGSILDWarning, CompactedEntity?> {
        if (localEntity == null && remoteEntitiesWithCSR.isEmpty()) return Ior.Right(null)

        val mergedEntity: MutableMap<String, Any> = localEntity?.toMutableMap() ?: mutableMapOf()
        val remoteEntitiesWithExpiresAtPropagated = remoteEntitiesWithCSR.map { (entity, csr) ->
            Pair(propagateExpiresAtToAttributes(entity), csr)
        }

        val warnings = remoteEntitiesWithExpiresAtPropagated.sortedBy { (_, csr) -> csr.isAuxiliary() }
            .mapNotNull { (entity, csr) ->
                getMergeNewValues(mergedEntity, entity, csr)
                    .onRight { mergedEntity.putAll(it) }.leftOrNull()
            }.toNonEmptyListOrNull()

        val sourceEntities = listOfNotNull(localEntity) + remoteEntitiesWithExpiresAtPropagated.map { it.first }
        resolveEntityExpiresAtConflict(mergedEntity, sourceEntities)

        return if (warnings == null) Ior.Right(mergedEntity) else Ior.Both(warnings, mergedEntity)
    }

    internal fun getMergeNewValues(
        currentEntity: CompactedEntity,
        remoteEntity: CompactedEntity,
        csr: ContextSourceRegistration
    ): Either<NGSILDWarning, CompactedEntity> = either {
        remoteEntity.mapValues { (key, value) ->
            val currentValue = currentEntity[key]
            when {
                currentValue == null -> value
                // expiresAt will be overwritten by resolveEntityExpiresAtConflict once every CSR has been merged
                // in (see 4.5.5.3), so whichever value is kept in this intermediate step is only a placeholder
                key == NGSILD_ID_TERM || key == JSONLD_CONTEXT_KW || key == NGSILD_EXPIRES_AT_TERM -> currentValue
                key == NGSILD_TYPE_TERM || key == NGSILD_SCOPE_TERM ->
                    mergeTypeOrScope(currentValue, value)
                key == NGSILD_CREATED_AT_TERM ->
                    if ((value as String?).isBefore(currentValue as String?)) value
                    else currentValue
                key == NGSILD_MODIFIED_AT_TERM ->
                    if ((currentValue as String?).isBefore(value as String?)) value
                    else currentValue
                else -> mergeAttribute(
                    currentValue,
                    value,
                    csr
                ).bind()
            }
        }
    }

    internal fun mergeTypeOrScope(
        currentValue: Any, // String || List<String> || Set<String>
        remoteValue: Any
    ) = when {
        currentValue == remoteValue -> currentValue
        currentValue is List<*> && remoteValue is List<*> -> (currentValue.toSet() + remoteValue.toSet()).toList()
        currentValue is List<*> -> (currentValue.toSet() + remoteValue).toList()
        remoteValue is List<*> -> (remoteValue.toSet() + currentValue).toList()
        else -> listOf(currentValue, remoteValue)
    }

    /**
     * Implements 4.5.5 - Multi-Attribute Support
     */
    fun mergeAttribute(
        currentAttribute: Any,
        remoteAttribute: Any,
        csr: ContextSourceRegistration
    ): Either<NGSILDWarning, Any> = either {
        val currentInstances = groupInstancesByDataSetId(currentAttribute, csr).bind().toMutableMap()
        val remoteInstances = groupInstancesByDataSetId(remoteAttribute, csr).bind()
        remoteInstances.entries.forEach { (datasetId, remoteInstance) ->
            val currentInstance = currentInstances[datasetId]
            currentInstances[datasetId] =
                if (currentInstance == null) remoteInstance
                else chooseAttributeInstance(currentInstance, remoteInstance, csr)
        }
        val values = currentInstances.values.toList()
        if (values.size == 1) values[0] else values
    }

    private fun chooseAttributeInstance(
        currentInstance: CompactedAttributeInstance,
        remoteInstance: CompactedAttributeInstance,
        csr: ContextSourceRegistration
    ): CompactedAttributeInstance {
        val isCurrentExpired = isAttributeInstanceExpired(currentInstance)
        val isRemoteExpired = isAttributeInstanceExpired(remoteInstance)
        return when {
            // 4.5.5.3 - discard whichever instance has an expiresAt DateTime that lies in the past
            isRemoteExpired && !isCurrentExpired -> currentInstance
            isCurrentExpired && !isRemoteExpired -> remoteInstance
            csr.isAuxiliary() -> currentInstance
            currentInstance.isBefore(remoteInstance, NGSILD_OBSERVED_AT_TERM) -> remoteInstance
            remoteInstance.isBefore(currentInstance, NGSILD_OBSERVED_AT_TERM) -> currentInstance
            currentInstance.isBefore(remoteInstance, NGSILD_MODIFIED_AT_TERM) -> remoteInstance
            remoteInstance.isBefore(currentInstance, NGSILD_MODIFIED_AT_TERM) -> currentInstance
            // if there is no discriminating factor, choose the current one
            else -> currentInstance
        }
    }

    // only meant to work with attributes under:
    // - the normalized representation when retrieving or querying entities
    // - the simplified or aggregated representation when retrieving or querying temporal entities
    private fun groupInstancesByDataSetId(
        attribute: Any,
        csr: ContextSourceRegistration
    ): Either<NGSILDWarning, AttributeByDatasetId> =
        when (attribute) {
            is Map<*, *> -> {
                attribute as CompactedAttributeInstance
                mapOf(attribute[NGSILD_DATASET_ID_TERM] as? String to attribute).right()
            }
            is List<*> -> {
                attribute as CompactedAttributeInstances
                attribute.associateBy { it[NGSILD_DATASET_ID_TERM] as? String }.right()
            }
            else -> {
                RevalidationFailedWarning( // could be avoided if Json payload is validated beforehand
                    contextSourceInvalidPayloadMessage(csr.id, attribute),
                    csr
                ).left()
            }
        }

    private fun CompactedAttributeInstance.isBefore(
        attr: CompactedAttributeInstance,
        property: String
    ): Boolean {
        val propertyTime = this[property] as String?
        val newPropertyTime = attr[property] as String?
        return when {
            propertyTime.isNullOrBlank() -> !newPropertyTime.isNullOrBlank()
            newPropertyTime.isNullOrBlank() -> false
            else -> propertyTime.isBefore(newPropertyTime)
        }
    }

    private fun String?.isBefore(date: String?) =
        this?.isDateTime() == true &&
            date?.isDateTime() == true &&
            ZonedDateTime.parse(this) < ZonedDateTime.parse(date)

    /**
     * Implements 4.5.5.3 - Processing of Conflicting Attributes: an attribute instance whose expiresAt lies in the
     * past shall be discarded when resolving a conflict against another instance of the same datasetId.
     */
    private fun isAttributeInstanceExpired(instance: CompactedAttributeInstance): Boolean {
        val expiresAt = instance[NGSILD_EXPIRES_AT_TERM] as? String ?: return false
        return expiresAt.isDateTime() && ZonedDateTime.parse(expiresAt) < ngsiLdDateTime()
    }

    fun mergeTemporalEntitiesLists(
        localEntities: List<CompactedEntity>,
        remoteEntitiesWithCSR: List<CompactedEntitiesWithCSR>,
        temporalEntitiesQuery: TemporalEntitiesQueryFromGet
    ): IorNel<NGSILDWarning, List<CompactedEntity>> {
        val mergedEntities = localEntities.map { it.toMutableMap() }.associateBy { it[NGSILD_ID_TERM] }.toMutableMap()
        val remoteEntitiesWithExpiresAtPropagated = remoteEntitiesWithCSR.map { (entities, csr) ->
            Pair(entities.map { propagateExpiresAtToAttributes(it) }, csr)
        }

        val warnings = remoteEntitiesWithExpiresAtPropagated.sortedBy { (_, csr) -> csr.isAuxiliary() }
            .mapNotNull { (entities, csr) ->
                either {
                    entities.forEach { entity ->
                        val id = entity[NGSILD_ID_TERM]
                        mergedEntities[id]
                            ?.let {
                                it.putAll(getMergeTemporalNewValues(it, entity, temporalEntitiesQuery, csr).bind())
                            }
                            ?: run { mergedEntities[id] = entity.toMutableMap() }
                    }
                    null
                }.leftOrNull()
            }.toNonEmptyListOrNull()

        val sourceEntitiesById = (localEntities + remoteEntitiesWithExpiresAtPropagated.flatMap { it.first })
            .groupBy { it[NGSILD_ID_TERM] }
        mergedEntities.forEach { (id, mergedEntity) ->
            resolveEntityExpiresAtConflict(mergedEntity, sourceEntitiesById[id] ?: emptyList())
        }

        val entities = mergedEntities.values.toList()
        return if (warnings == null) Ior.Right(entities) else Ior.Both(warnings, entities)
    }

    fun mergeTemporalEntities(
        localEntity: CompactedEntity?,
        remoteEntitiesWithCSR: List<CompactedEntityWithCSR>,
        temporalEntitiesQuery: TemporalEntitiesQueryFromGet
    ): IorNel<NGSILDWarning, CompactedEntity?> {
        if (localEntity == null && remoteEntitiesWithCSR.isEmpty()) return Ior.Right(null)

        val mergedEntity: MutableMap<String, Any> = localEntity?.toMutableMap() ?: mutableMapOf()
        val remoteEntitiesWithExpiresAtPropagated = remoteEntitiesWithCSR.map { (entity, csr) ->
            Pair(propagateExpiresAtToAttributes(entity), csr)
        }

        val warnings = remoteEntitiesWithExpiresAtPropagated.sortedBy { (_, csr) -> csr.isAuxiliary() }
            .mapNotNull { (entity, csr) ->
                getMergeTemporalNewValues(mergedEntity, entity, temporalEntitiesQuery, csr)
                    .onRight { mergedEntity.putAll(it) }.leftOrNull()
            }.toNonEmptyListOrNull()

        val sourceEntities = listOfNotNull(localEntity) + remoteEntitiesWithExpiresAtPropagated.map { it.first }
        resolveEntityExpiresAtConflict(mergedEntity, sourceEntities)

        return if (warnings == null)
            Ior.Right(mergedEntity)
        else Ior.Both(warnings, mergedEntity)
    }

    internal fun getMergeTemporalNewValues(
        currentEntity: CompactedEntity,
        remoteEntity: CompactedEntity,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        csr: ContextSourceRegistration
    ): Either<NGSILDWarning, CompactedEntity> = either {
        remoteEntity.mapValues { (key, value) ->
            val currentValue = currentEntity[key]
            when {
                currentValue == null -> value
                key == NGSILD_ID_TERM || key == JSONLD_CONTEXT_KW -> currentValue
                key == NGSILD_TYPE_TERM -> mergeTypeOrScope(currentValue, value)
                // expiresAt will be overwritten by resolveEntityExpiresAtConflict once every CSR has been merged
                // in (see 4.5.5.3), so whichever value the "earliest wins" rule below picks for it here is only
                // a placeholder
                key in NGSILD_SYSATTRS_TERMS ->
                    if ((value as String?).isBefore(currentValue as String?)) value
                    else currentValue
                // handles temporal attributes and scope
                else -> mergeTemporalAttribute(currentValue, value, temporalEntitiesQuery, csr).bind()
            }
        }
    }

    fun mergeTemporalAttribute(
        currentAttribute: Any,
        remoteAttribute: Any,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        csr: ContextSourceRegistration
    ): Either<NGSILDWarning, Any> = either {
        when (temporalEntitiesQuery.temporalRepresentation) {
            TemporalRepresentation.NORMALIZED ->
                mergeNormalizeAttributeInstances(
                    currentAttribute,
                    remoteAttribute,
                    temporalEntitiesQuery.temporalQuery,
                    csr
                )
            TemporalRepresentation.TEMPORAL_VALUES ->
                mergeSimplifiedOrAggregatedAttributeInstances(
                    currentAttribute,
                    remoteAttribute,
                    TEMPORAL_REPRESENTATION_TERMS,
                    csr
                )
            TemporalRepresentation.AGGREGATED_VALUES ->
                mergeSimplifiedOrAggregatedAttributeInstances(
                    currentAttribute,
                    remoteAttribute,
                    TemporalQuery.Aggregate.toMethodsNames(),
                    csr
                )
        }
    }

    private fun Raise<NGSILDWarning>.mergeNormalizeAttributeInstances(
        currentAttribute: Any,
        remoteAttribute: Any,
        temporalQuey: TemporalQuery,
        csr: ContextSourceRegistration
    ): List<CompactedAttributeInstance> {
        val currentInstances = toListOfNormalizedInstances(currentAttribute, csr).bind()
        val remoteInstances = toListOfNormalizedInstances(remoteAttribute, csr).bind()

        val timePropertyName = temporalQuey.timeproperty.propertyName
        val keysInCurrentInstances = currentInstances.map {
            it[timePropertyName] to it[NGSILD_DATASET_ID_TERM]
        }.toSet()

        val newInstances = remoteInstances.filter {
            it[timePropertyName] to it[NGSILD_DATASET_ID_TERM] !in keysInCurrentInstances
        }

        return if (newInstances.isEmpty()) {
            currentInstances
        } else {
            (currentInstances + newInstances).sortedBy {
                // order instances by the asked time property
                it[timePropertyName] as String
            }
        }
    }

    private fun Raise<NGSILDWarning>.mergeSimplifiedOrAggregatedAttributeInstances(
        currentAttribute: Any,
        remoteAttribute: Any,
        keysToMerge: List<String>,
        csr: ContextSourceRegistration
    ): Any {
        val currentInstances = groupInstancesByDataSetId(currentAttribute, csr).bind().toMutableMap()
        val remoteInstances = groupInstancesByDataSetId(remoteAttribute, csr).bind()
        val currentAndRemoteValues = currentInstances.map { (datasetId, currentInstance) ->
            val remoteInstance = remoteInstances[datasetId]
            if (remoteInstance != null) {
                currentInstance.mapValues { (key, value) ->
                    if (key in keysToMerge && remoteInstance.containsKey(key)) {
                        val temporalValues = value as List<*>
                        val remoteTemporalValues = remoteInstance[key] as List<*>
                        val timestampsInCurrentInstances = temporalValues.map { (it as List<*>)[1] as String }.toSet()
                        val newTemporalValues = remoteTemporalValues.filter {
                            (it as List<*>)[1] as String !in timestampsInCurrentInstances
                        }
                        if (newTemporalValues.isEmpty())
                            value
                        else
                            (temporalValues + newTemporalValues).sortedBy {
                                // order instances using 2nd element of the list
                                // - timestamp for simplified representation
                                // - start of aggregation for aggregated representation
                                (it as List<*>)[1] as String
                            }
                    } else value
                }
            } else currentInstance
        }
        val remoteOnlyValues = remoteInstances.filter { (datasetId, _) ->
            datasetId !in currentInstances.keys
        }.values.toList()

        return (currentAndRemoteValues + remoteOnlyValues).let {
            if (it.size == 1) it[0] else it
        }
    }

    private fun toListOfNormalizedInstances(
        attribute: Any,
        csr: ContextSourceRegistration
    ): Either<NGSILDWarning, List<CompactedAttributeInstance>> =
        when (attribute) {
            is Map<*, *> -> listOf(attribute as CompactedAttributeInstance).right()
            is List<*> -> (attribute as CompactedAttributeInstances).right()
            else -> RevalidationFailedWarning(contextSourceInvalidPayloadMessage(csr.id, attribute), csr).left()
        }
}
