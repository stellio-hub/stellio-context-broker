package com.egm.stellio.search.csr.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.fasterxml.jackson.annotation.JsonIgnore

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
