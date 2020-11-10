package com.egm.stellio.entity.model

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonValue
import java.net.URI

data class UpdateResult(
    val updated: List<UpdatedDetails>,
    val notUpdated: List<NotUpdatedDetails>
)

data class NotUpdatedDetails(
    val attributeName: String,
    val reason: String
)

data class UpdatedDetails(
    @JsonValue
    val attributeName: String,
    @JsonIgnore
    val datasetId: URI?,
    @JsonIgnore
    val updateOperationResult: UpdateOperationResult
)

data class UpdateAttributeResult(
    val attributeName: String,
    val datasetId: URI?,
    val updateOperationResult: UpdateOperationResult,
    val errorMessage: String? = null
) {
    fun isSuccessfullyUpdated() =
        this.updateOperationResult in listOf(UpdateOperationResult.APPENDED, UpdateOperationResult.REPLACED)
}

enum class UpdateOperationResult {
    APPENDED,
    REPLACED,
    IGNORED
}
