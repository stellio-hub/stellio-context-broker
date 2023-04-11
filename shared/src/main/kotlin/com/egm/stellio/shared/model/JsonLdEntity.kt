package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.castAttributeValue
import java.time.ZonedDateTime

typealias CompactedJsonLdEntity = Map<String, Any>

data class JsonLdEntity(
    val members: Map<String, Any>,
    val contexts: List<String>
) {
    fun containsAnyOf(expandedAttributes: Set<String>): Boolean =
        expandedAttributes.isEmpty() || members.keys.any { expandedAttributes.contains(it) }

    fun checkContainsAnyOf(expandedAttributes: Set<String>): Either<APIException, Unit> =
        if (containsAnyOf(expandedAttributes))
            Unit.right()
        else ResourceNotFoundException(entityOrAttrsNotFoundMessage(id, expandedAttributes)).left()

    fun getAttributes(): ExpandedAttributes =
        members.filter { !JsonLdUtils.JSONLD_EXPANDED_ENTITY_MANDATORY_FIELDS.contains(it.key) }
            .mapValues { castAttributeValue(it.value) }

    // called at entity creation time to populate entity and attributes with createdAt information
    fun populateCreatedAt(createdAt: ZonedDateTime): JsonLdEntity =
        JsonLdEntity(
            members = members.mapValues {
                if (JsonLdUtils.JSONLD_EXPANDED_ENTITY_MANDATORY_FIELDS.contains(it.key))
                    it.value
                else castAttributeValue(it.value).map { expandedAttributeInstance ->
                    expandedAttributeInstance.addDateTimeProperty(
                        NGSILD_CREATED_AT_PROPERTY,
                        createdAt
                    ) as ExpandedAttributeInstance
                }
            }.addDateTimeProperty(NGSILD_CREATED_AT_PROPERTY, createdAt),
            contexts = contexts
        )

    // FIXME kinda hacky but we often just need the id or type... how can it be improved?
    val id by lazy {
        (members[JSONLD_ID] ?: InternalErrorException("Could not extract id from JSON-LD entity")) as String
    }

    val types by lazy {
        (members[JSONLD_TYPE] ?: InternalErrorException("Could not extract type from JSON-LD entity"))
            as List<ExpandedTerm>
    }
}
