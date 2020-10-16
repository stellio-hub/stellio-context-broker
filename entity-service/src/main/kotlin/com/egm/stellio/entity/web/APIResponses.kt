package com.egm.stellio.entity.web

import java.net.URI

data class BatchOperationResult(
    val success: MutableList<URI> = mutableListOf(),
    val errors: MutableList<BatchEntityError> = mutableListOf()
) {

    operator fun plusAssign(other: BatchOperationResult) {
        success.addAll(other.success)
        errors.addAll(other.errors)
    }
}

data class BatchEntityError(
    val entityId: URI,
    val error: MutableList<String>
)

const val ENTITY_NOT_FOUND_MESSAGE = "Entity Not Found"
