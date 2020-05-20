package com.egm.stellio.entity.web

data class BatchOperationResult(
    val success: ArrayList<String>,
    val errors: ArrayList<BatchEntityError>
) {

    operator fun plusAssign(other: BatchOperationResult) {
        success.addAll(other.success)
        errors.addAll(other.errors)
    }
}

data class BatchEntityError(
    val entityId: String,
    val error: ArrayList<String>

)