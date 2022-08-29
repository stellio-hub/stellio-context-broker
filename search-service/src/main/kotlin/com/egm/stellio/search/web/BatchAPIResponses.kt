package com.egm.stellio.search.web

import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.shared.model.NgsiLdEntity
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
