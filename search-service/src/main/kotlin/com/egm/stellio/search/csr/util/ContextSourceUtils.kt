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
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedAttributeInstances
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.model.NGSILD_SYSATTRS_TERMS
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.TEMPORAL_REPRESENTATION_TERMS
import com.egm.stellio.shared.util.ErrorMessages.Csr.contextSourceInvalidPayloadMessage
import com.egm.stellio.shared.util.isDateTime
import java.time.ZonedDateTime
import kotlin.random.Random.Default.nextBoolean

typealias CompactedEntityWithCSR = Pair<CompactedEntity, ContextSourceRegistration>
typealias CompactedEntitiesWithCSR = Pair<List<CompactedEntity>, ContextSourceRegistration>

typealias AttributeByDatasetId = Map<String?, CompactedAttributeInstance>

object ContextSourceUtils {

    fun mergeEntitiesLists(
        localEntities: List<CompactedEntity>,
        remoteEntitiesWithCSR: List<CompactedEntitiesWithCSR>
    ): IorNel<NGSILDWarning, List<CompactedEntity>> {
        val mergedEntityMap = localEntities.map { it.toMutableMap() }.associateBy { it[NGSILD_ID_TERM] }.toMutableMap()

        val warnings = remoteEntitiesWithCSR.sortedBy { (_, csr) -> csr.isAuxiliary() }.mapNotNull { (entities, csr) ->
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

        val entities = mergedEntityMap.values.toList()
        return if (warnings == null) Ior.Right(entities) else Ior.Both(warnings, entities)
    }

    fun mergeEntities(
        localEntity: CompactedEntity?,
        remoteEntitiesWithCSR: List<CompactedEntityWithCSR>
    ): IorNel<NGSILDWarning, CompactedEntity?> {
        if (localEntity == null && remoteEntitiesWithCSR.isEmpty()) return Ior.Right(null)

        val mergedEntity: MutableMap<String, Any> = localEntity?.toMutableMap() ?: mutableMapOf()

        val warnings = remoteEntitiesWithCSR.sortedBy { (_, csr) -> csr.isAuxiliary() }
            .mapNotNull { (entity, csr) ->
                getMergeNewValues(mergedEntity, entity, csr)
                    .onRight { mergedEntity.putAll(it) }.leftOrNull()
            }.toNonEmptyListOrNull()

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
                key == NGSILD_ID_TERM || key == JSONLD_CONTEXT_KW -> currentValue
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
            when {
                currentInstance == null -> currentInstances[datasetId] = remoteInstance
                csr.isAuxiliary() -> Unit
                currentInstance.isBefore(remoteInstance, NGSILD_OBSERVED_AT_TERM) ->
                    currentInstances[datasetId] = remoteInstance
                remoteInstance.isBefore(currentInstance, NGSILD_OBSERVED_AT_TERM) -> Unit
                currentInstance.isBefore(remoteInstance, NGSILD_MODIFIED_AT_TERM) ->
                    currentInstances[datasetId] = remoteInstance
                remoteInstance.isBefore(currentInstance, NGSILD_MODIFIED_AT_TERM) -> Unit
                // if there is no discriminating factor choose one at random
                nextBoolean() -> currentInstances[datasetId] = remoteInstance
                else -> Unit
            }
        }
        val values = currentInstances.values.toList()
        if (values.size == 1) values[0] else values
    }

    // only meant to work with attributes under:
    // - the normalized representation when retrieving or querying entities
    // - the simplified or aggregated representation when retrieving or querying tmeporal entities
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
    ): Boolean = (this[property] as String?)?.isBefore(attr[property] as String?) == true

    private fun String?.isBefore(date: String?) =
        this?.isDateTime() == true &&
            date?.isDateTime() == true &&
            ZonedDateTime.parse(this) < ZonedDateTime.parse(date)

    fun mergeTemporalEntitiesLists(
        localEntities: List<CompactedEntity>,
        remoteEntitiesWithCSR: List<CompactedEntitiesWithCSR>,
        temporalRepresentation: TemporalRepresentation
    ): IorNel<NGSILDWarning, List<CompactedEntity>> {
        val mergedEntityMap = localEntities.map { it.toMutableMap() }.associateBy { it[NGSILD_ID_TERM] }.toMutableMap()

        val warnings = remoteEntitiesWithCSR.sortedBy { (_, csr) -> csr.isAuxiliary() }.mapNotNull { (entities, csr) ->
            either {
                entities.forEach { entity ->
                    val id = entity[NGSILD_ID_TERM]
                    mergedEntityMap[id]
                        ?.let { it.putAll(getMergeTemporalNewValues(it, entity, temporalRepresentation, csr).bind()) }
                        ?: run { mergedEntityMap[id] = entity.toMutableMap() }
                }
                null
            }.leftOrNull()
        }.toNonEmptyListOrNull()

        val entities = mergedEntityMap.values.toList()
        return if (warnings == null) Ior.Right(entities) else Ior.Both(warnings, entities)
    }

    fun mergeTemporalEntities(
        localEntity: CompactedEntity?,
        remoteEntitiesWithCSR: List<CompactedEntityWithCSR>,
        temporalRepresentation: TemporalRepresentation
    ): IorNel<NGSILDWarning, CompactedEntity?> {
        if (localEntity == null && remoteEntitiesWithCSR.isEmpty()) return Ior.Right(null)

        val mergedEntity: MutableMap<String, Any> = localEntity?.toMutableMap() ?: mutableMapOf()

        val warnings = remoteEntitiesWithCSR.sortedBy { (_, csr) -> csr.isAuxiliary() }
            .mapNotNull { (entity, csr) ->
                getMergeTemporalNewValues(mergedEntity, entity, temporalRepresentation, csr)
                    .onRight { mergedEntity.putAll(it) }.leftOrNull()
            }.toNonEmptyListOrNull()

        return if (warnings == null)
            Ior.Right(mergedEntity)
        else Ior.Both(warnings, mergedEntity)
    }

    internal fun getMergeTemporalNewValues(
        currentEntity: CompactedEntity,
        remoteEntity: CompactedEntity,
        temporalRepresentation: TemporalRepresentation,
        csr: ContextSourceRegistration
    ): Either<NGSILDWarning, CompactedEntity> = either {
        remoteEntity.mapValues { (key, value) ->
            val currentValue = currentEntity[key]
            when {
                currentValue == null -> value
                key == NGSILD_ID_TERM || key == JSONLD_CONTEXT_KW -> currentValue
                key == NGSILD_TYPE_TERM -> mergeTypeOrScope(currentValue, value)
                key in NGSILD_SYSATTRS_TERMS ->
                    if ((value as String?).isBefore(currentValue as String?)) value
                    else currentValue
                // handles temporal attributes and scope
                else -> mergeTemporalAttribute(currentValue, value, temporalRepresentation, csr).bind()
            }
        }
    }

    fun mergeTemporalAttribute(
        currentAttribute: Any,
        remoteAttribute: Any,
        temporalRepresentation: TemporalRepresentation,
        csr: ContextSourceRegistration
    ): Either<NGSILDWarning, Any> = either {
        when (temporalRepresentation) {
            TemporalRepresentation.NORMALIZED -> {
                val currentInstances = toListOfNormalizedInstances(currentAttribute, csr).bind()
                val remoteInstances = toListOfNormalizedInstances(remoteAttribute, csr).bind()
                currentInstances + remoteInstances
            }
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

    private fun Raise<NGSILDWarning>.mergeSimplifiedOrAggregatedAttributeInstances(
        currentAttribute: Any,
        remoteAttribute: Any,
        keysToMerge: List<String>,
        csr: ContextSourceRegistration
    ): Any {
        val currentInstances = groupInstancesByDataSetId(currentAttribute, csr).bind().toMutableMap()
        val remoteInstances = groupInstancesByDataSetId(remoteAttribute, csr).bind()
        val currentAndRemoteValues = currentInstances.entries.map { (datasetId, currentInstance) ->
            val remoteInstance = remoteInstances[datasetId]
            if (remoteInstance != null) {
                currentInstance.mapValues { (key, value) ->
                    if (key in keysToMerge) {
                        if (remoteInstance.containsKey(key))
                            (value as List<*>).plus(remoteInstance[key] as List<*>)
                        else value
                    } else value
                }
            } else currentInstance
        }
        val remoteOnlyValues = remoteInstances.filter { (datasetId, _) ->
            datasetId !in currentInstances.map { it.key }
        }.map { it.value }

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
