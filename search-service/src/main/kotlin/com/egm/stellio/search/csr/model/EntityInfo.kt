package com.egm.stellio.search.csr.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.areTypesInSelection
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.toTypeSelection
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.util.CollectionUtils
import org.springframework.util.MultiValueMap
import java.net.URI
import java.util.regex.Pattern

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
        this.id?.let {
            csrFilters.ids.isEmpty() || csrFilters.ids.contains(it)
        } ?: true &&
            csrFilters.typeSelection?.let { typeSelection ->
                areTypesInSelection(this.types, typeSelection)
            } ?: true &&
            this.idPattern?.let { pattern ->
                csrFilters.ids.isEmpty() || csrFilters.ids.any { pattern.toRegex().matches(it.toString()) }
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
