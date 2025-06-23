package com.egm.stellio.search.csr.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.DataTypes.convertTo
import com.egm.stellio.shared.util.DataTypes.serialize
import com.egm.stellio.shared.util.DataTypes.toFinalRepresentation
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CSR_TERM
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.invalidUriMessage
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.http.MediaType
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

typealias SingleEntityInfoCSR = ContextSourceRegistration

/**
 * CSourceRegistration type as defined in 5.2.9
 */
data class ContextSourceRegistration(
    val id: URI = "urn:ngsi-ld:ContextSourceRegistration:${UUID.randomUUID()}".toUri(),
    val endpoint: URI,
    val registrationName: String? = null,
    val type: String = NGSILD_CSR_TERM,
    val mode: Mode = Mode.INCLUSIVE,
    val information: List<RegistrationInfo> = emptyList(),
    val operations: List<Operation> = listOf(Operation.FEDERATION_OPS),
    val createdAt: ZonedDateTime = ngsiLdDateTime(),
    val modifiedAt: ZonedDateTime = createdAt,
    val observationInterval: TimeInterval? = null,
    val managementInterval: TimeInterval? = null,
    val status: StatusType? = null,
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    val timesSent: Int = 0,
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    val timesFailed: Int = 0,
    val lastFailure: ZonedDateTime? = null,
    val lastSuccess: ZonedDateTime? = null,
) {
    @JsonIgnore
    fun isAuxiliary(): Boolean = mode == Mode.AUXILIARY

    @JsonIgnore
    fun isMatchingOperation(operation: Operation): Boolean =
        operations.any { it in operation.getMatchingOperations() }

    data class TimeInterval(
        val start: ZonedDateTime,
        val end: ZonedDateTime? = null
    )

    fun expand(contexts: List<String>): ContextSourceRegistration =
        this.copy(information = information.map { it.expand(contexts) })

    fun compact(contexts: List<String>): ContextSourceRegistration =
        this.copy(information = information.map { it.compact(contexts) })

    fun serialize(
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String {
        return serialize(
            convertTo<Map<String, Any>>(
                this.compact(contexts)
            ).plus(
                JSONLD_CONTEXT to contexts
            ).let { toFinalRepresentation(it, mediaType, includeSysAttrs) }
        )
    }

    fun validate() = either {
        checkTypeIsContextSourceRegistration().bind()
        checkIdIsValid().bind()
        information.map { it.validate().bind() }
    }

    private fun checkTypeIsContextSourceRegistration(): Either<APIException, Unit> =
        if (type != NGSILD_CSR_TERM)
            BadRequestDataException("type attribute must be equal to 'ContextSourceRegistration'").left()
        else Unit.right()

    private fun checkIdIsValid(): Either<APIException, Unit> =
        if (!id.isAbsolute)
            BadRequestDataException(invalidUriMessage("$id")).left()
        else Unit.right()

    @JsonIgnore
    fun getAttributesMatchingCSFAndEntity(
        csrFilters: CSRFilters,
        entity: ExpandedEntity,
    ): Set<ExpandedTerm> {
        val matchingRegistrationsInfo = getMatchingInformation(csrFilters)

        val attributes =
            if (matchingRegistrationsInfo.any { it.getAttributeNames() == null }) null
            else matchingRegistrationsInfo.flatMap { it.getAttributeNames()!! }.toSet()

        return entity.getAttributes().filter { (expandedTerm, _) ->
            attributes == null || expandedTerm in attributes
        }.keys
    }

    fun toSingleEntityInfoCSRList(csrFilters: CSRFilters): List<SingleEntityInfoCSR> =
        this.information.flatMap { registrationInfo ->
            registrationInfo.entities?.map {
                this.copy(information = listOf(registrationInfo.copy(entities = listOf(it))))
            } ?: listOf(this.copy(information = listOf(registrationInfo)))
        }.filter { it.matchCSF(csrFilters) }

    private fun getMatchingInformation(csrFilters: CSRFilters): List<RegistrationInfo> =
        information.filter { it.matchCSF(csrFilters) }

    private fun matchCSF(csrFilters: CSRFilters): Boolean = information.any { it.matchCSF(csrFilters) }

    companion object {

        fun deserialize(
            input: Map<String, Any>,
            contexts: List<String>
        ): Either<APIException, ContextSourceRegistration> =
            runCatching {
                deserializeAs<ContextSourceRegistration>(serializeObject(input.plus(JSONLD_CONTEXT to contexts)))
                    .expand(contexts)
            }.fold(
                { it.right() },
                { it.toAPIException("Failed to parse CSourceRegistration caused by :\n ${it.message}").left() }
            )

        fun notFoundMessage(id: URI) = "Could not find a CSourceRegistration with id $id"
        fun alreadyExistsMessage(id: URI) = "A CSourceRegistration with id $id already exists"
        fun unauthorizedMessage(id: URI) = "User is not authorized to access CSourceRegistration $id"
    }

    enum class StatusType(val status: String) {
        @JsonProperty("ok")
        OK("ok"),

        @JsonProperty("failed")
        FAILED("failed")
    }
}

fun List<ContextSourceRegistration>.serialize(
    contexts: List<String>,
    mediaType: MediaType = JSON_LD_MEDIA_TYPE,
    includeSysAttrs: Boolean = false
): String = this.map {
    it.serialize(contexts, mediaType, includeSysAttrs)
}.toString()
