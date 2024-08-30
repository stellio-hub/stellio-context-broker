package com.egm.stellio.search.csr.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CSR_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.module.kotlin.convertValue
import org.springframework.http.MediaType
import java.net.URI
import java.time.ZonedDateTime
import java.util.*
import java.util.regex.Pattern

data class ContextSourceRegistration(
    val id: URI = "urn:ngsi-ld:ContextSourceRegistration:${UUID.randomUUID()}".toUri(),
    val endpoint: URI,
    val type: String = NGSILD_CSR_TERM,
    val mode: Mode = Mode.INCLUSIVE,
    val information: List<RegistrationInfo> = emptyList(),
    val operations: List<Operation> = listOf(Operation.FEDERATION_OPS),
    val createdAt: ZonedDateTime = ngsiLdDateTime(),
    val modifiedAt: ZonedDateTime? = null,
    val observationInterval: TimeInterval? = null,
    val managementInterval: TimeInterval? = null
) {

    data class TimeInterval(
        val start: ZonedDateTime,
        val end: ZonedDateTime? = null
    )

    data class RegistrationInfo(
        val entities: List<EntityInfo>?,
        val propertyNames: List<String>?,
        val relationshipNames: List<String>?
    ) {
        fun expand(contexts: List<String>): RegistrationInfo =
            RegistrationInfo(
                entities = entities?.map { it.expand(contexts) },
                propertyNames = propertyNames?.map { expandJsonLdTerm(it, contexts) },
                relationshipNames = relationshipNames?.map { expandJsonLdTerm(it, contexts) },
            )

        fun compact(contexts: List<String>): RegistrationInfo =
            RegistrationInfo(
                entities = entities?.map { it.compact(contexts) },
                propertyNames = propertyNames?.map { compactTerm(it, contexts) },
                relationshipNames = relationshipNames?.map { compactTerm(it, contexts) }
            )

        fun validate(): Either<BadRequestDataException, Unit> = either {
            return if (entities != null || propertyNames != null || relationshipNames != null) {
                entities?.forEach { it.validate().bind() }
                Unit.right()
            } else {
                BadRequestDataException("RegistrationInfo should have at least one element").left()
            }
        }
    }

    data class EntityInfo(
        val id: URI? = null,
        val idPattern: String? = null,
        @JsonFormat(
            with = [
                JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
                JsonFormat.Feature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED
            ]
        )
        val type: List<String>
    ) {
        fun expand(contexts: List<String>): EntityInfo =
            this.copy(
                type = type.map { expandJsonLdTerm(it, contexts) },
            )

        fun compact(contexts: List<String>): EntityInfo =
            this.copy(
                type = type.map { compactTerm(it, contexts) },
            )

        fun validate(): Either<BadRequestDataException, Unit> {
            val result = runCatching {
                idPattern?.let { Pattern.compile(it) }
                true
            }.fold({ true }, { false })

            return if (result)
                Unit.right()
            else BadRequestDataException("Invalid idPattern found in contextSourceRegistration").left()
        }
    }
    fun expand(contexts: List<String>): ContextSourceRegistration =
        this.copy(
            information = information.map { it.expand(contexts) }
        )

    fun compact(contexts: List<String>): ContextSourceRegistration =
        this.copy(
            information = information.map { it.compact(contexts) }
        )

    fun serialize(
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String {
        return DataTypes.mapper.writeValueAsString(
            DataTypes.mapper.convertValue<Map<String, Any>>(
                (this.compact(contexts))
            ).plus(
                JSONLD_CONTEXT to contexts
            ).let { DataTypes.toFinalRepresentation(it, mediaType, includeSysAttrs) }
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
}

fun List<ContextSourceRegistration>.serialize(
    contexts: List<String>,
    mediaType: MediaType = JSON_LD_MEDIA_TYPE,
    includeSysAttrs: Boolean = false
): String = this.map {
    it.serialize(contexts, mediaType, includeSysAttrs)
}.toString()
