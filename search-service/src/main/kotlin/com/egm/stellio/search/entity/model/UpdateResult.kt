package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonValue
import java.net.URI

/**
 * UpdateResult datatype as defined in 5.2.18
 */
data class UpdateResult(
    val updated: List<UpdatedDetails>,
    val notUpdated: List<NotUpdatedDetails>
) {

    @JsonIgnore
    fun isSuccessful(): Boolean =
        notUpdated.isEmpty()
}

val EMPTY_UPDATE_RESULT: UpdateResult = UpdateResult(emptyList(), emptyList())

/**
 * NotUpdatedDetails as defined in 5.2.19
 */
data class NotUpdatedDetails(
    val attributeName: String,
    val reason: String
)

/**
 * UpdatedDetails as defined in 5.2.18
 */
data class UpdatedDetails(
    @JsonValue
    val attributeName: String
)

/**
 * Internal structure used to convey the result of an operation (update, delete...)
 */
sealed class AttributeOperationResult(
    open val attributeName: String,
    open val datasetId: URI? = null,
    open val operationStatus: OperationStatus
)

data class SucceededAttributeOperationResult(
    override val attributeName: String,
    override val datasetId: URI? = null,
    override val operationStatus: OperationStatus,
    val newExpandedValue: ExpandedAttributeInstance,
) : AttributeOperationResult(attributeName, datasetId, operationStatus)

data class FailedAttributeOperationResult(
    override val attributeName: String,
    override val datasetId: URI? = null,
    override val operationStatus: OperationStatus,
    val errorMessage: String
) : AttributeOperationResult(attributeName, datasetId, operationStatus)

enum class OperationStatus {
    APPENDED,
    REPLACED,
    UPDATED,
    DELETED,
    IGNORED,
    FAILED;

    fun isSuccessResult(): Boolean = getSuccessStatuses().contains(this)

    companion object {
        fun getSuccessStatuses(): List<OperationStatus> = listOf(APPENDED, REPLACED, UPDATED, DELETED, IGNORED)
    }
}

fun List<AttributeOperationResult>.hasSuccessfulResult(): Boolean =
    this.any { it is SucceededAttributeOperationResult }

fun List<AttributeOperationResult>.getSucceededOperations(): List<SucceededAttributeOperationResult> =
    this.filterIsInstance<SucceededAttributeOperationResult>()

fun updateResultFromDetailedResult(updateStatuses: List<AttributeOperationResult>): UpdateResult =
    updateStatuses.map {
        when (it) {
            is SucceededAttributeOperationResult -> UpdatedDetails(it.attributeName)
            is FailedAttributeOperationResult -> NotUpdatedDetails(it.attributeName, it.errorMessage)
        }
    }.let {
        UpdateResult(
            it.filterIsInstance<UpdatedDetails>(),
            it.filterIsInstance<NotUpdatedDetails>()
        )
    }
