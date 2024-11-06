package com.egm.stellio.search.csr.service

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedAttributeInstances
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.util.isDateTime
import java.time.ZonedDateTime
import kotlin.random.Random.Default.nextBoolean

typealias CompactedEntityWithCSR = Pair<CompactedEntity, ContextSourceRegistration>
typealias AttributeByDatasetId = Map<String?, CompactedAttributeInstance>
object ContextSourceUtils {

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
                key == JSONLD_ID_TERM || key == JSONLD_CONTEXT -> currentValue
                key == JSONLD_TYPE_TERM || key == NGSILD_SCOPE_TERM ->
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
                csr.isAuxiliary() -> {}
                currentInstance.isBefore(remoteInstance, NGSILD_OBSERVED_AT_TERM) ->
                    currentInstances[datasetId] = remoteInstance
                remoteInstance.isBefore(currentInstance, NGSILD_OBSERVED_AT_TERM) -> {}
                currentInstance.isBefore(remoteInstance, NGSILD_MODIFIED_AT_TERM) ->
                    currentInstances[datasetId] = remoteInstance
                remoteInstance.isBefore(currentInstance, NGSILD_MODIFIED_AT_TERM) -> {}
                // if there is no discriminating factor choose one at random
                nextBoolean() -> currentInstances[datasetId] = remoteInstance
                else -> {}
            }
        }
        val values = currentInstances.values.toList()
        if (values.size == 1) values[0] else values
    }

    // only meant to work with attributes under the normalized representation
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
                RevalidationFailedWarning(
                    "The received payload is invalid. Attribute is nor List nor a Map : $attribute",
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
}
