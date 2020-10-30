package com.egm.stellio.entity.model

import java.net.URI

data class UpdateAttributesResponse(
    val updated: List<String>,
    val notUpdated: List<NotUpdatedAttributeDetails>
)
data class UpdateAttributesResult(
    val updated: List<UpdatedAttributeDetails>,
    val notUpdated: List<NotUpdatedAttributeDetails>
)

data class UpdatedAttributeDetails(
    val attributeName: String,
    val datasetId: URI?,
    val updateOperationResult: UpdateOperationResult
)

data class NotUpdatedAttributeDetails(
    val attributeName: String,
    val reason: String
)

data class UpdateAttributeResult(
    val attributeName: String,
    val datasetId: URI?,
    val updateOperationResult: UpdateOperationResult,
    val errorMessage: String?
) {
    fun isSuccessfullyUpdated() =
        this.updateOperationResult in listOf(UpdateOperationResult.APPENDED, UpdateOperationResult.REPLACED)
}

enum class UpdateOperationResult {
    APPENDED,
    REPLACED,
    IGNORED
}
