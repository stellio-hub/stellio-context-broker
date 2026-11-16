package com.egm.stellio.search.csr.util

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.discovery.model.AttributeDetails
import com.egm.stellio.search.discovery.model.AttributeList
import com.egm.stellio.search.discovery.model.AttributeTypeInfo
import com.egm.stellio.search.discovery.model.EntityType
import com.egm.stellio.search.discovery.model.EntityTypeInfo
import com.egm.stellio.search.discovery.model.EntityTypeList
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.compactTermWithCache
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTermWithCache

// Restrict the answer of a Context Source to the types and attributes declared in its registration.
// Names in the answer are compacted with the request contexts: they are expanded to be compared with the
// registration, and display names are recomputed from the FQN ids.

fun EntityTypeList.restrictTo(csr: ContextSourceRegistration, contexts: List<String>): EntityTypeList {
    val expand = termExpander(contexts)
    return copy(typeList = typeList.filter { csr.isTypeAllowed(expand(it)) })
}

@JvmName("restrictEntityTypesTo")
fun List<EntityType>.restrictTo(csr: ContextSourceRegistration, contexts: List<String>): List<EntityType> {
    val expand = termExpander(contexts)
    val compact = termCompacter(contexts)
    return filter { csr.isTypeAllowed(it.id.toString()) }.mapNotNull { entityType ->
        val type = entityType.id.toString()
        entityType.attributeNames
            .keepAllowedOrDropParent { csr.isAttributeAllowed(expand(it), type) }
            ?.let { entityType.copy(typeName = compact(type), attributeNames = it) }
    }
}

// the CSR was selected on the requested type, only an answer about another type has to be rejected
fun EntityTypeInfo.restrictTo(
    type: ExpandedTerm,
    csr: ContextSourceRegistration,
    contexts: List<String>
): EntityTypeInfo? {
    if (id.toString() != type) return null
    val compact = termCompacter(contexts)
    return attributeDetails
        .keepAllowedOrDropParent { csr.isAttributeAllowed(it.id.toString(), type) }
        ?.let { attributeDetails ->
            copy(
                typeName = compact(type),
                attributeDetails = attributeDetails.map { it.copy(attributeName = compact(it.id.toString())) }
            )
        }
}

fun AttributeList.restrictTo(csr: ContextSourceRegistration, contexts: List<String>): AttributeList {
    val expand = termExpander(contexts)
    return copy(attributeList = attributeList.filter { csr.isAttributeAllowed(expand(it)) })
}

@JvmName("restrictAttributesDetailsTo")
fun List<AttributeDetails>.restrictTo(csr: ContextSourceRegistration, contexts: List<String>): List<AttributeDetails> {
    val expand = termExpander(contexts)
    val compact = termCompacter(contexts)
    return filter { csr.isAttributeAllowed(it.id.toString()) }.mapNotNull { attributeDetails ->
        val attribute = attributeDetails.id.toString()
        attributeDetails.typeNames.toList()
            .keepAllowedOrDropParent { csr.isAttributeAllowed(attribute, expand(it)) }
            ?.let { attributeDetails.copy(attributeName = compact(attribute), typeNames = it.toSet()) }
    }
}

// the CSR was selected on the requested attribute, only an answer about another attribute has to be rejected
fun AttributeTypeInfo.restrictTo(
    attribute: ExpandedTerm,
    csr: ContextSourceRegistration,
    contexts: List<String>
): AttributeTypeInfo? {
    if (id.toString() != attribute) return null
    val expand = termExpander(contexts)
    return typeNames.toList()
        .keepAllowedOrDropParent { csr.isAttributeAllowed(attribute, expand(it)) }
        ?.let { copy(attributeName = termCompacter(contexts)(attribute), typeNames = it.toSet()) }
}

// null when the list had elements but none of them is allowed: the parent element is then not exposed either
private fun <E> List<E>.keepAllowedOrDropParent(isAllowed: (E) -> Boolean): List<E>? {
    if (isEmpty()) return this
    val allowed = filter(isAllowed)
    return allowed.ifEmpty { null }
}

// the same names come back many times in an answer (e.g. a type in the typeNames of each attribute)
private fun termExpander(contexts: List<String>): (String) -> ExpandedTerm {
    val cache = mutableMapOf<String, ExpandedTerm>()
    return { term -> expandJsonLdTermWithCache(term, contexts, cache) }
}

private fun termCompacter(contexts: List<String>): (ExpandedTerm) -> String {
    val cache = mutableMapOf<ExpandedTerm, String>()
    return { term -> compactTermWithCache(term, contexts, cache) }
}
