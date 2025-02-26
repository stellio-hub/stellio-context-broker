package com.egm.stellio.search.csr.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.areTypesInSelection
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CSR_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.invalidUriMessage
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toTypeSelection
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.convertValue
import org.springframework.http.MediaType
import org.springframework.util.CollectionUtils
import org.springframework.util.MultiValueMap
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID
import java.util.regex.Pattern

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

    /**
     * RegistrationInfo type as defined in 5.2.10
     */
    data class RegistrationInfo(
        val entities: List<EntityInfo>? = null,
        val propertyNames: List<String>? = null,
        val relationshipNames: List<String>? = null
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

        @JsonIgnore
        fun getAttributeNames(): Set<String>? = when {
            this.propertyNames == null && this.relationshipNames == null -> null
            this.propertyNames == null -> this.relationshipNames?.toSet()
            this.relationshipNames == null -> this.propertyNames.toSet()
            else -> this.propertyNames.toMutableSet().plus(this.relationshipNames.toSet())
        }

        fun matchCSF(csrFilters: CSRFilters): Boolean =
            entities?.any { entityInfo ->
                entityInfo.matchCSF(csrFilters)
            } ?: true &&
                (
                    csrFilters.attrs.isEmpty() ||
                        getAttributeNames()?.any { it in csrFilters.attrs } ?: true
                    )

        @JsonIgnore
        fun computeAttrsQueryParam(
            csrFilters: CSRFilters,
            contexts: List<String>
        ): String? {
            val csrAttrs = getAttributeNames()
            val queryAttrs = csrFilters.attrs
            val matchingAttrs = when {
                queryAttrs.isEmpty() -> csrAttrs
                csrAttrs.isNullOrEmpty() -> queryAttrs
                else -> csrAttrs.intersect(queryAttrs)
            }
            return if (matchingAttrs.isNullOrEmpty()) null
            else matchingAttrs.joinToString(",") { compactTerm(it, contexts) }
        }
    }

    /**
     * EntityInfo type as defined in 5.2.8
     */
    data class EntityInfo(
        val id: URI? = null,
        val idPattern: String? = null,
        // no WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED because it is used for the database
        @JsonFormat(with = [JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY])
        @JsonProperty("type")
        val types: List<String>
    ) {
        fun expand(contexts: List<String>): EntityInfo =
            this.copy(
                types = types.map { expandJsonLdTerm(it, contexts) },
            )

        fun compact(contexts: List<String>): EntityInfo =
            this.copy(
                types = types.map { compactTerm(it, contexts) },
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

        fun matchCSF(csrFilters: CSRFilters) =
            this.id?.let { csrFilters.ids.contains(it) } ?: true &&
                csrFilters.typeSelection?.let { typeSelection ->
                    areTypesInSelection(this.types, typeSelection)
                } ?: true &&
                this.idPattern?.let { pattern ->
                    csrFilters.ids.any { pattern.toRegex().matches(it.toString()) }
                } ?: true

        companion object {

            fun MultiValueMap<String, String>.addFilterForEntityInfo(
                entityInfo: EntityInfo
            ): MultiValueMap<String, String> {
                val newParams = CollectionUtils.toMultiValueMap(this.toMutableMap())
                newParams[QP.TYPE.key] = entityInfo.types.toTypeSelection()
                entityInfo.id?.let { id -> newParams[QP.ID.key] = id.toString() }
                if (this.getFirst(QP.ID_PATTERN.key) == null && entityInfo.idPattern != null)
                    newParams[QP.ID_PATTERN.key] = entityInfo.idPattern
                return newParams
            }
        }
    }

    fun expand(contexts: List<String>): ContextSourceRegistration =
        this.copy(information = information.map { it.expand(contexts) })

    fun compact(contexts: List<String>): ContextSourceRegistration =
        this.copy(information = information.map { it.compact(contexts) })

    fun serialize(
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String {
        return DataTypes.mapper.writeValueAsString(
            DataTypes.mapper.convertValue<Map<String, Any>>(
                this.compact(contexts)
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
