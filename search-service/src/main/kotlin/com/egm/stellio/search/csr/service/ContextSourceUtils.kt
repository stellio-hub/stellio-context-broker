package com.egm.stellio.search.csr.service

import arrow.core.*
import arrow.core.raise.either
import arrow.core.raise.iorNel
import com.egm.stellio.search.csr.model.*
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedAttributeInstances
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_TERM
import java.time.ZonedDateTime
import kotlin.random.Random.Default.nextBoolean

typealias CompactedEntityWithMode = Pair<CompactedEntity, Mode>
typealias DataSetId = String?
typealias AttributeByDataSetId = Map<DataSetId, CompactedAttributeInstance>
object ContextSourceUtils {

    fun mergeEntities(
        localEntity: CompactedEntity?,
        remoteEntitiesWithMode: List<CompactedEntityWithMode>
    ): IorNel<NGSILDWarning, CompactedEntity?> = iorNel {
        if (localEntity == null && remoteEntitiesWithMode.isEmpty()) return@iorNel null

        val mergedEntity: MutableMap<String, Any> = localEntity?.toMutableMap() ?: mutableMapOf()

        remoteEntitiesWithMode.sortedBy { (_, mode) -> mode == Mode.AUXILIARY }
            .forEach { (entity, mode) ->
                mergedEntity.putAll(
                    getMergeNewValues(mergedEntity, entity, mode).toIor().toIorNel().bind()
                )
            }

        return@iorNel mergedEntity.toMap()
    }

    private fun getMergeNewValues(
        localEntity: CompactedEntity,
        remoteEntity: CompactedEntity,
        mode: Mode
    ): Either<NGSILDWarning, CompactedEntity> = either {
        remoteEntity.mapValues { (key, value) ->
            val localValue = localEntity[key]
            when {
                localValue == null -> value
                key == JSONLD_ID_TERM || key == JSONLD_CONTEXT -> localValue
                key == JSONLD_TYPE_TERM || key == NGSILD_SCOPE_TERM ->
                    mergeTypeOrScope(localValue, value)
                key == NGSILD_CREATED_AT_TERM ->
                    if ((value as String?).isBefore(localValue as String?)) value
                    else localValue
                key == NGSILD_MODIFIED_AT_TERM ->
                    if ((localValue as String?).isBefore(value as String?)) value
                    else localValue
                else -> mergeAttribute(
                    localValue,
                    value,
                    mode == Mode.AUXILIARY
                ).bind()
            }
        }
    }

    fun mergeTypeOrScope(
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
        isAuxiliary: Boolean = false
    ): Either<NGSILDWarning, Any> = either {
        val currentInstances = groupInstancesByDataSetId(currentAttribute).bind().toMutableMap()
        val remoteInstances = groupInstancesByDataSetId(remoteAttribute).bind()
        remoteInstances.entries.forEach { (datasetId, remoteInstance) ->
            val currentInstance = currentInstances[datasetId]
            when {
                currentInstance == null -> currentInstances[datasetId] = remoteInstance
                isAuxiliary -> {}
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

    // do not work with CORE MEMBER since they are nor list nor map
    private fun groupInstancesByDataSetId(attribute: Any): Either<NGSILDWarning, AttributeByDataSetId> =
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
                    "The received payload is invalid. Attribute is nor List nor a Map : $attribute"
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
