package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_EXPANDED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.entityOrAttrsNotFoundMessage
import com.egm.stellio.shared.util.toUri
import java.net.URI
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

    fun filterOnAttributes(includedAttributes: Set<String>, includedDatasetIds: Set<URI>?): Map<String, Any> {
        val inputToMap = { i: ExpandedEntity -> i.members }
        return filterEntityOnAttributes(this, inputToMap, includedAttributes, includedDatasetIds)
    }

    private fun filterEntityOnAttributes(

        input: ExpandedEntity,
        inputToMap: (ExpandedEntity) -> Map<String, Any>,
        includedAttributes: Set<String>,
        includedDatasetIds: Set<URI>?,
    ): Map<String, Any> {
        var result = inputToMap(input)

            if (includedAttributes.isEmpty() && includedDatasetIds == null) {
                result = inputToMap(input)
            }

        if (includedAttributes.isNotEmpty()) {
            val includedKeys = JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.plus(includedAttributes)
            result = inputToMap(input).filterKeys { includedKeys.contains(it) }
        }
        if (includedDatasetIds != null && includedDatasetIds.isNotEmpty()) {
                val includedKeys = JSONLD_EXPANDED_ENTITY_CORE_MEMBERS
                result = ((result) as ExpandedAttributes).mapNotNull { entry ->

                    if (entry.key in includedKeys) {
                        return@mapNotNull entry.key to entry.value
                    }

                    val filteredEntry = entry.value.filter { instance ->
                        ((instance[NGSILD_DATASET_ID_PROPERTY] as Map<String, String>)[JSONLD_ID]?.toUri()!!) in includedDatasetIds
                    }
                    return@mapNotNull entry.key to filteredEntry
                }.toMap()
        }
        return result
    }
}

fun List<ExpandedEntity>.filterOnAttributes(includedAttributes: Set<String>, includedDatasetIds: Set<URI>?): List<ExpandedEntity> =
    this.map {
        ExpandedEntity(it.filterOnAttributes(includedAttributes, includedDatasetIds))
    }
