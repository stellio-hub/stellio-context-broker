package com.egm.stellio.search.service.execution.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_SERVICE_EXECUTION_TERM
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.ErrorMessages.GenericValidation.invalidTypeMessage
import com.egm.stellio.shared.util.ErrorMessages.GenericValidation.invalidUriMessage
import com.egm.stellio.shared.util.ErrorMessages.GenericValidation.memberIsInvalidMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.SERVICE_EXECUTION_UPDATE_MEMBERS_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.serviceExecutionFailedToParseMessage
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.http.MediaType
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class ServiceExecution(
    val id: URI = "urn:ngsi-ld:ServiceExecution:${UUID.randomUUID()}".toUri(),
    val type: String = NGSILD_SERVICE_EXECUTION_TERM,
    val serviceId: URI,
    val entityId: URI,
    val entityType: String,
    val input: Any,
    /** Name-based service resolution is not implemented; [serviceId] is used instead. */
    val serviceName: String? = null,
    val executionStatus: ServiceExecutionStatus = ServiceExecutionStatus.PENDING,
    val completion: Double? = null,
    val output: Any? = null,
    val responseStatusCode: Int? = null,
    val createdAt: ZonedDateTime = ngsiLdDateTime(),
    val modifiedAt: ZonedDateTime = createdAt,
) {
    fun expand(contexts: List<String>): ServiceExecution =
        copy(entityType = expandJsonLdTerm(entityType, contexts))

    fun compact(contexts: List<String>): ServiceExecution =
        copy(entityType = compactTerm(entityType, contexts))

    fun serialize(
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String =
        DataTypes.convertTo<Map<String, Any>>(compact(contexts))
            .plus(JSONLD_CONTEXT_KW to contexts)
            .let { DataTypes.toFinalRepresentation(it, mediaType, includeSysAttrs) }
            .let(DataTypes::serialize)

    fun validate(): Either<APIException, Unit> = either {
        if (type != NGSILD_SERVICE_EXECUTION_TERM)
            BadRequestDataException(invalidTypeMessage(NGSILD_SERVICE_EXECUTION_TERM)).left().bind<Unit>()
        if (!id.isAbsolute)
            BadRequestDataException(invalidUriMessage(id.toString())).left().bind<Unit>()
        if (!entityId.isAbsolute)
            BadRequestDataException(invalidUriMessage(entityId.toString())).left().bind<Unit>()
        if (!serviceId.isAbsolute)
            BadRequestDataException(invalidUriMessage(serviceId.toString())).left().bind<Unit>()
        if (entityType.isBlank())
            BadRequestDataException(memberIsInvalidMessage("entityType")).left().bind<Unit>()
        if (completion?.let { !it.isFinite() || it !in 0.0..1.0 } == true)
            BadRequestDataException(memberIsInvalidMessage("completion")).left().bind<Unit>()
    }

    fun mergeWithFragment(
        fragment: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, ServiceExecution> = either {
        ensure((fragment.keys - JSONLD_CONTEXT_KW).all(PATCHABLE_MEMBERS::contains)) {
            BadRequestDataException(SERVICE_EXECUTION_UPDATE_MEMBERS_MESSAGE)
        }
        val mergedExecution = DataTypes.convertTo<Map<String, Any>>(this@ServiceExecution).plus(fragment)
        deserialize(mergedExecution, contexts).bind()
            .copy(modifiedAt = ngsiLdDateTime())
    }

    companion object {
        private val PATCHABLE_MEMBERS = setOf("completion", "output", "executionStatus")

        fun deserialize(
            input: Map<String, Any>,
            contexts: List<String>
        ): Either<APIException, ServiceExecution> =
            runCatching {
                deserializeAs<ServiceExecution>(serializeObject(input.plus(JSONLD_CONTEXT_KW to contexts)))
                    .expand(contexts)
            }.fold(
                { it.right() },
                { it.toAPIException(serviceExecutionFailedToParseMessage(it.message)).left() }
            )
    }
}

enum class ServiceExecutionStatus {
    @JsonProperty("pending")
    PENDING,

    @JsonProperty("executing")
    EXECUTING,

    @JsonProperty("success")
    SUCCESS,

    @JsonProperty("failure")
    FAILURE,

    @JsonProperty("cancelled")
    CANCELLED
}
