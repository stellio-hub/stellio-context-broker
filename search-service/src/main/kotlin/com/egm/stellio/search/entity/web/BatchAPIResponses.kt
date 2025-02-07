package com.egm.stellio.search.entity.web

import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.toErrorResponse
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonValue
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import java.net.URI

/**
 * BatchOperationResult type as defined in 5.2.16
 */
data class BatchOperationResult(
    val success: MutableList<BatchEntitySuccess> = mutableListOf(),
    val errors: MutableList<BatchEntityError> = mutableListOf()
) {

    operator fun plusAssign(other: BatchOperationResult) {
        success.addAll(other.success)
        errors.addAll(other.errors)
    }

    @JsonIgnore
    fun getSuccessfulEntitiesIds() = success.map { it.entityId }

    @JsonIgnore
    fun addEntitiesToErrors(entities: List<Pair<String, APIException>>) =
        entities.forEach {
            errors.add(BatchEntityError(it.first.toUri(), it.second.toProblemDetail()))
        }

    // the BatchOperationResult is also used for distributed provision operation
    // for those endpoint you return a single error if the all operation failed at once
    fun toNonBatchEndpointResponse(entityId: URI): ResponseEntity<*> {
        val location = URI("/ngsi-ld/v1/entities/$entityId")
        return when {
            this.errors.isEmpty() ->
                ResponseEntity.status(HttpStatus.CREATED)
                    .location(location)
                    .build<String>()

            this.success.isEmpty() && this.errors.size == 1 ->
                this.errors.first().error.toErrorResponse()

            else ->
                ResponseEntity.status(HttpStatus.MULTI_STATUS)
                    .location(location)
                    .body(this)
        }
    }
}

data class BatchEntitySuccess(
    @JsonValue
    val entityId: URI,
    @JsonIgnore
    val updateResult: UpdateResult? = null
)

/**
 * BatchEntityError type as defined in 5.2.17
 */
data class BatchEntityError(
    val entityId: URI,
    val error: ProblemDetail,
    val registrationId: URI? = null
)

typealias JsonLdNgsiLdEntity = Pair<ExpandedEntity, NgsiLdEntity>

fun JsonLdNgsiLdEntity.entityId(): URI = this.second.id

// a temporary data class to hold the result of deserializing, expanding and transforming to NGSI-LD entities
// the entities received in a batch operation
data class BatchEntityPreparation(
    val success: List<JsonLdNgsiLdEntity> = emptyList(),
    val errors: List<Pair<String, APIException>> = emptyList()
)
