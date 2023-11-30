package com.egm.stellio.search.web

import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonValue
import java.net.URI

data class BatchOperationResult(
    val success: MutableList<BatchEntitySuccess> = mutableListOf(),
    val errors: MutableList<BatchEntityError> = mutableListOf()
) {

    operator fun plusAssign(other: BatchOperationResult) {
        success.addAll(other.success)
        errors.addAll(other.errors)
    }

    @JsonIgnore
    fun getSuccessfulEntitiesIds() = success.map { it.entityId }

    @JsonIgnore
    fun addEntitiesToErrors(entities: List<Pair<String, APIException>>) =
        entities.forEach {
            errors.add(BatchEntityError(it.first.toUri(), arrayListOf(it.second.message)))
        }

    @JsonIgnore
    fun addEntitiesToErrors(entities: List<NgsiLdEntity>, errorMessage: String) =
        addIdsToErrors(entities.map { it.id }, errorMessage)

    @JsonIgnore
    fun addIdsToErrors(entitiesIds: List<URI>, errorMessage: String) =
        errors.addAll(
            entitiesIds.map { BatchEntityError(it, arrayListOf(errorMessage)) }
        )
}

data class BatchEntitySuccess(
    @JsonValue
    val entityId: URI,
    @JsonIgnore
    val updateResult: UpdateResult? = null
)

data class BatchEntityError(
    val entityId: URI,
    val error: MutableList<String>
)

typealias JsonLdNgsiLdEntity = Pair<JsonLdEntity, NgsiLdEntity>

fun List<JsonLdNgsiLdEntity>.extractNgsiLdEntities(): List<NgsiLdEntity> = this.map { it.second }
fun JsonLdNgsiLdEntity.entityId(): URI = this.second.id

// a temporary data class to hold the result of deserializing, expanding and transforming to NGSI-LD entities
// the entities received in a batch operation
data class BatchEntityPreparation(
    val success: List<JsonLdNgsiLdEntity> = emptyList(),
    val errors: List<Pair<String, APIException>> = emptyList()
)
