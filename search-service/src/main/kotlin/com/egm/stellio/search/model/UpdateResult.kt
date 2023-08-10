package com.egm.stellio.search.model

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonValue
import java.net.URI

data class UpdateResult(
    val updated: List<UpdatedDetails>,
    val notUpdated: List<NotUpdatedDetails>
) {

    @JsonIgnore
    fun isSuccessful(): Boolean =
        notUpdated.isEmpty() &&
            updated.all { it.updateOperationResult.isSuccessResult() }

    @JsonIgnore
    fun mergeWith(other: UpdateResult): UpdateResult =
        UpdateResult(
            updated = this.updated.plus(other.updated),
            notUpdated = this.notUpdated.plus(other.notUpdated)
        )
}

val EMPTY_UPDATE_RESULT: UpdateResult = UpdateResult(emptyList(), emptyList())

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
    val datasetId: URI? = null,
    val updateOperationResult: UpdateOperationResult,
    val errorMessage: String? = null
) {
    fun isSuccessfullyUpdated() =
        this.updateOperationResult in listOf(
            UpdateOperationResult.APPENDED,
            UpdateOperationResult.REPLACED,
            UpdateOperationResult.UPDATED
        )
}

enum class UpdateOperationResult {
    APPENDED,
    REPLACED,
    UPDATED,
    IGNORED,
    FAILED;

    fun isSuccessResult(): Boolean = listOf(APPENDED, REPLACED, UPDATED).contains(this)
}

fun UpdateResult.hasSuccessfulUpdate(): Boolean =
    this.updated.isNotEmpty()

fun updateResultFromDetailedResult(updateStatuses: List<UpdateAttributeResult>): UpdateResult {
    val updated = updateStatuses.filter { it.isSuccessfullyUpdated() }
        .map { UpdatedDetails(it.attributeName, it.datasetId, it.updateOperationResult) }

    val notUpdated = updateStatuses.filter { !it.isSuccessfullyUpdated() }
        .map { NotUpdatedDetails(it.attributeName, it.errorMessage!!) }

    return UpdateResult(updated, notUpdated)
}
