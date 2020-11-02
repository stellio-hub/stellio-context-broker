package com.egm.stellio.entity.web

import com.egm.stellio.entity.model.UpdateAttributesResult
import com.egm.stellio.entity.model.UpdatedAttributeDetails
import java.net.URI

data class BatchOperationResponse(
    val success: MutableList<URI> = mutableListOf(),
    val errors: MutableList<BatchEntityError> = mutableListOf()
)

data class BatchOperationResult(
    val success: MutableList<BatchEntitySuccess> = mutableListOf(),
    val errors: MutableList<BatchEntityError> = mutableListOf()
){
    operator fun plusAssign(other: BatchOperationResult) {
        success.addAll(other.success)
        errors.addAll(other.errors)
    }
}

data class BatchEntityError(
    val entityId: URI,
    val error: MutableList<String>
)

open class BatchEntitySuccess(
    val batchOperationType: BatchEntityOperationType,
    open val entityId: URI
)

data class BatchEntityCreateSuccess(
    override val entityId: URI
) : BatchEntitySuccess(BatchEntityOperationType.ENTITY_CREATE, entityId)

data class BatchEntityDeleteSuccess(
    override val entityId: URI
) : BatchEntitySuccess(BatchEntityOperationType.ENTITY_DELETE, entityId)

data class BatchEntityReplaceSuccess(
    override val entityId: URI,
    val updateAttributesResult: List<UpdatedAttributeDetails>
) : BatchEntitySuccess(BatchEntityOperationType.ENTITY_REPLACE, entityId)

data class BatchEntityUpdateSuccess(
    override val entityId: URI,
    val updateAttributesResult: UpdateAttributesResult
) : BatchEntitySuccess(BatchEntityOperationType.ENTITY_UPDATE, entityId)


enum class BatchEntityOperationType {
    ENTITY_CREATE,
    ENTITY_DELETE,
    ENTITY_REPLACE,
    ENTITY_UPDATE
}
