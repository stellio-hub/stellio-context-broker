package com.egm.stellio.entity.web

data class BatchOperationResult(
    val success: MutableList<String> = mutableListOf(),
    val errors: MutableList<BatchEntityError> = mutableListOf()
) {

    operator fun plusAssign(other: BatchOperationResult) {
        success.addAll(other.success)
        errors.addAll(other.errors)
    }
}

data class BatchEntityError(
    val entityId: String,
    val error: MutableList<String>

)
