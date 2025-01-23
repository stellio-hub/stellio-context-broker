package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.left
import arrow.core.mapValuesNotNull
import arrow.core.right
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_EXPANDED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NONE_TERM
import com.egm.stellio.shared.util.entityOrAttrsNotFoundMessage
import java.time.ZonedDateTime

data class ExpandedEntity(
    val members: Map<String, Any>
) {
    fun containsAnyOf(expandedAttributes: Set<String>): Boolean =
        expandedAttributes.isEmpty() || members.keys.any { expandedAttributes.contains(it) }

    fun checkContainsAnyOf(expandedAttributes: Set<String>): Either<APIException, Unit> =
        if (containsAnyOf(expandedAttributes))
            Unit.right()
        else ResourceNotFoundException(entityOrAttrsNotFoundMessage(id, expandedAttributes)).left()

    fun asNonCoreAttributes(): Boolean = members.keys.any {
        !JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it)
    }

    fun getAttributes(): ExpandedAttributes =
        members.filter { !JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it.key) }
            .mapValues { castAttributeValue(it.value) }

    fun getModifiableMembers(): ExpandedAttributes =
        members.filter { !listOf(JSONLD_ID, JSONLD_CONTEXT).contains(it.key) }
            .mapValues { castAttributeValue(it.value) }

    fun getScopes(): List<String>? =
        (members as Map<String, List<Any>>).getScopes()

    /**
     * Called at entity creation time to populate entity and attributes with createdAt information
     */
    fun populateCreationTimeDate(createdAt: ZonedDateTime): ExpandedEntity =
        ExpandedEntity(
            members = members.mapValues {
                if (JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it.key))
                    it.value
                else castAttributeValue(it.value).map { expandedAttributeInstance ->
                    expandedAttributeInstance.addDateTimeProperty(
                        NGSILD_CREATED_AT_PROPERTY,
                        createdAt
                    ) as ExpandedAttributeInstance
                }
            }.addDateTimeProperty(NGSILD_CREATED_AT_PROPERTY, createdAt)
        )

    /**
     * Called when replacing entity to populate entity and attributes with createdAt and modifiedAt information
     * for attributes, the modification date is added as the creation date
     */
    fun populateReplacementTimeDates(createdAt: ZonedDateTime, replacedAt: ZonedDateTime): ExpandedEntity =
        ExpandedEntity(
            members = members.mapValues {
                if (JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it.key))
                    it.value
                else castAttributeValue(it.value).map { expandedAttributeInstance ->
                    expandedAttributeInstance.addDateTimeProperty(
                        NGSILD_CREATED_AT_PROPERTY,
                        replacedAt
                    ) as ExpandedAttributeInstance
                }
            }
                .addDateTimeProperty(NGSILD_CREATED_AT_PROPERTY, createdAt)
                .addDateTimeProperty(NGSILD_MODIFIED_AT_PROPERTY, replacedAt)
        )

    val id by lazy {
        (members[JSONLD_ID] ?: throw BadRequestDataException("Could not extract id from JSON-LD entity")) as String
    }

    val types by lazy {
        (members[JSONLD_TYPE] ?: throw BadRequestDataException("Could not extract type from JSON-LD entity"))
            as List<ExpandedTerm>
    }

    private fun Map<String, Any>.addDateTimeProperty(propertyKey: String, dateTime: ZonedDateTime?): Map<String, Any> =
        if (dateTime != null)
            this.plus(propertyKey to JsonLdUtils.buildNonReifiedTemporalValue(dateTime))
        else this

    fun filterAttributes(
        includedAttributes: Set<String>,
        includedDatasetIds: Set<String>,
    ): ExpandedEntity = ExpandedEntity(
        if (includedAttributes.isEmpty() && includedDatasetIds.isEmpty()) {
            members
        } else
            members.filterKeys {
                includedAttributes.isEmpty() ||
                    JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.plus(includedAttributes).contains(it)
            }.mapValuesNotNull { entry ->
                if (entry.key in JSONLD_EXPANDED_ENTITY_CORE_MEMBERS)
                    entry.value
                else (entry.value as ExpandedAttributeInstances).filter { expandedAttributeInstance ->
                    includedDatasetIds.isEmpty() ||
                        includedDatasetIds.contains(NGSILD_NONE_TERM) &&
                        expandedAttributeInstance.getDatasetId() == null ||
                        expandedAttributeInstance.getDatasetId() != null &&
                        includedDatasetIds.contains(expandedAttributeInstance.getDatasetId().toString())
                }.ifEmpty { null }
            }
    )

    fun omitAttributes(attributes: Set<String>): ExpandedEntity = ExpandedEntity(
        members.filterKeys { it !in attributes }
    )
}

fun List<ExpandedEntity>.filterAttributes(
    includedAttributes: Set<String>,
    includedDatasetIds: Set<String>
): List<ExpandedEntity> =
    this.map {
        it.filterAttributes(includedAttributes, includedDatasetIds)
    }
