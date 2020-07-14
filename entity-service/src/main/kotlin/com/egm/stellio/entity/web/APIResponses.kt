package com.egm.stellio.entity.web

data class BatchOperationResult(
    val success: ArrayList<String> = arrayListOf(),
    val errors: ArrayList<BatchEntityError> = arrayListOf()
) {

    operator fun plusAssign(other: BatchOperationResult) {
        success.addAll(other.success)
        errors.addAll(other.errors)
    }
}

data class BatchEntityError(
    val entityId: String,
    val error: List<String>
)
