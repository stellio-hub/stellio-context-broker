package com.egm.stellio.entity.web

import com.egm.stellio.entity.model.UpdateResult
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

    fun getSuccessfulEntitiesIds() = success.map { it.entityId }
}

data class BatchEntitySuccess(
    @JsonValue
    val entityId: URI,
    @JsonIgnore
    val updateResult: UpdateResult? = null
)

data class BatchEntityError(
    val entityId: URI,
    val error: MutableList<String>,
    @JsonIgnore
    val updateResult: UpdateResult? = null
)
