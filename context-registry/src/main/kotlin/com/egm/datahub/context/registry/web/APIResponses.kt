package com.egm.datahub.context.registry.web

class BatchOperationResult(
    val success: ArrayList<String>,
    val errors: ArrayList<BatchEntityError>
)

class BatchEntityError(
    val entityId: String,
    val error: ArrayList<String>
)