package com.egm.stellio.shared.model

import arrow.core.Nel
import arrow.core.Validated
import arrow.core.extensions.nonemptylist.semigroup.semigroup
import arrow.core.extensions.validated.applicative.applicative
import arrow.core.fix
import arrow.core.invalid
import arrow.core.orNull
import arrow.core.valid
import arrow.core.validNel
import com.egm.stellio.shared.util.AttributeType
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandValueAsListOfMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandValueAsMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.isAttributeOfType
import com.egm.stellio.shared.util.NgsiLdParsingUtils.isValidAttribute
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor

// TODO we define a list of errors, this may not be necessary as we ultimately return the reason string to the consumers
sealed class EntityParsingError(val reason: String) {
    object MissingIdError : EntityParsingError("The provided NGSI-LD entity does not contain an id property")
    object MissingTypeError : EntityParsingError("The provided NGSI-LD entity does not contain a type property")
    class MixedAttributeInstancesError(reason: String) : EntityParsingError(reason)
    class NonUniqueDatasetIdError(reason: String) : EntityParsingError(reason)
    class MoreThanOneDefaultInstanceError(reason: String) : EntityParsingError(reason)

    override fun toString(): String = reason
}

data class JsonLdExpandedEntity(
    val rawJsonLdProperties: Map<String, Any>,
    val contexts: List<String>
) {
    // FIXME kinda hacky but we often just need the id ... how can it be improved?
    val id = rawJsonLdProperties[NGSILD_ENTITY_ID]!! as String

    fun compact(): Map<String, Any> =
        JsonLdProcessor.compact(rawJsonLdProperties, mapOf("@context" to contexts), JsonLdOptions())
}

typealias PropertyInstances = List<Map<String, List<Any>>>

data class ExpandedEntity(
    val id: String,
    val type: String,
    val properties: Map<String, PropertyInstances>,
    val relationships: Map<String, Map<String, List<Any>>>,
    val geoProperties: Map<String, Map<String, List<Any>>>,
    val contexts: List<String>
) {

    val attributes = properties.plus(relationships).plus(geoProperties)

    // TODO add validation for linked entities

    /**
     * Gets linked entities ids.
     * Entities can be linked either by a relation or a property.
     *
     * @return a subset of [entities]
     */
    fun getLinkedEntitiesIds(): List<String> =
        getLinkedEntitiesIdsByProperties().plus(getLinkedEntitiesIdsByRelations())

    private fun getLinkedEntitiesIdsByRelations(): List<String> {
        return relationships.map {
            NgsiLdParsingUtils.getRelationshipObjectId(it.value)
        }.plus(getLinkedEntitiesByRelationship(relationships))
    }

    private fun getLinkedEntitiesIdsByProperties(): List<String> {
        return getLinkedEntitiesByProperty(properties)
    }

    private fun getLinkedEntitiesByRelationship(attributes: Map<String, Map<String, List<Any>>>): List<String> {
        return attributes.flatMap { attribute ->
            attribute.value
                .map {
                    it.value[0]
                }.filterIsInstance<Map<String, List<Any>>>()
                .filter {
                    NgsiLdParsingUtils.isAttributeOfType(it, NGSILD_RELATIONSHIP_TYPE)
                }.map {
                    NgsiLdParsingUtils.getRelationshipObjectId(it)
                }
        }
    }

    private fun getLinkedEntitiesByProperty(attributes: Map<String, List<Map<String, List<Any>>>>): List<String> {
        return attributes.flatMap { attribute ->
            attribute.value.flatMap { instance ->
                instance.map {
                    it.value[0]
                }.filterIsInstance<Map<String, List<Any>>>()
                .filter {
                    NgsiLdParsingUtils.isAttributeOfType(it, NGSILD_RELATIONSHIP_TYPE)
                }.map {
                    NgsiLdParsingUtils.getRelationshipObjectId(it)
                }
            }
        }
    }
}

private val validatedApplicative = Validated.applicative<Nel<EntityParsingError>>(Nel.semigroup())

fun checkId(rawJsonLdProperties: Map<String, Any>): Validated<EntityParsingError, String> =
    if (!rawJsonLdProperties.containsKey("@id"))
        EntityParsingError.MissingIdError.invalid()
    else
        (rawJsonLdProperties[NGSILD_ENTITY_ID]!! as String).valid()

fun checkType(rawJsonLdProperties: Map<String, Any>): Validated<EntityParsingError, String> =
    if (!rawJsonLdProperties.containsKey("@type"))
        EntityParsingError.MissingTypeError.invalid()
    else
        (rawJsonLdProperties[NGSILD_ENTITY_TYPE]!! as List<String>)[0].valid()

fun getAttributesOfType(rawJsonLdProperties: Map<String, Any>, type: AttributeType): Any =
    rawJsonLdProperties.filterKeys {
        val entityCoreProperties = listOf(NGSILD_ENTITY_ID, NGSILD_ENTITY_TYPE).plus(NGSILD_ENTITY_CORE_MEMBERS)
        !entityCoreProperties.contains(it)
    }
    .filter {
        when (type) {
            NGSILD_PROPERTY_TYPE -> isAttributeOfType(expandValueAsListOfMap(it.value)[0], type)
            else -> isAttributeOfType(expandValueAsMap(it.value), type)
        }
    }

fun checkProperties(rawJsonLdProperties: Map<String, Any>): Validated<Nel<EntityParsingError>, Map<String, PropertyInstances>> {
    val rawProperties = getAttributesOfType(rawJsonLdProperties, NGSILD_PROPERTY_TYPE) as Map<String, PropertyInstances>
    val errors = arrayListOf<EntityParsingError>()
    rawProperties.forEach { (key, values) ->
        if (!isValidAttribute(key, values))
            errors.add(EntityParsingError.MixedAttributeInstancesError("${key.extractShortTypeFromExpanded()} attribute instances must have the same type"))
        if (values.count { !it.containsKey(NGSILD_DATASET_ID_PROPERTY) } > 1)
            errors.add(EntityParsingError.MoreThanOneDefaultInstanceError("Property ${key.extractShortTypeFromExpanded()} can't have more than one default instance"))
        val datasetIds = values.filter {
            it.containsKey(NGSILD_DATASET_ID_PROPERTY)
        }.map {
            val datasetId = it[NGSILD_DATASET_ID_PROPERTY]?.get(0) as Map<String, String>?
            datasetId?.get(NGSILD_ENTITY_ID)
        }
        if (datasetIds.toSet().count() != datasetIds.count())
            errors.add(EntityParsingError.NonUniqueDatasetIdError("Property ${key.extractShortTypeFromExpanded()} can't have duplicated datasetId"))
    }

    return if (errors.isNotEmpty())
        Nel.fromListUnsafe(errors).invalid()
    else rawProperties.valid()
}

// TODO add validation
fun checkRelationships(rawJsonLdProperties: Map<String, Any>): Validated<Nel<EntityParsingError>, Map<String, Map<String, List<Any>>>> {
    val rawRelationships = getAttributesOfType(rawJsonLdProperties, NGSILD_RELATIONSHIP_TYPE) as Map<String, Map<String, List<Any>>>
    return rawRelationships.valid()
}

// TODO add validation
fun checkGeoProperties(rawJsonLdProperties: Map<String, Any>): Validated<Nel<EntityParsingError>, Map<String, Map<String, List<Any>>>> {
    val rawGeoProperties = getAttributesOfType(rawJsonLdProperties, NGSILD_GEOPROPERTY_TYPE) as Map<String, Map<String, List<Any>>>
    return rawGeoProperties.valid()
}

fun JsonLdExpandedEntity.toEntity(): Validated<Nel<EntityParsingError>, ExpandedEntity> =
    validatedApplicative
        .tupled(
            checkId(this.rawJsonLdProperties).toValidatedNel(),
            checkType(this.rawJsonLdProperties).toValidatedNel(),
            checkProperties(this.rawJsonLdProperties),
            checkRelationships(this.rawJsonLdProperties),
            checkGeoProperties(this.rawJsonLdProperties)
        )
        .fix()
        .fold({ it.invalid() }, {
            (id, type, properties, relationships, geoProperties) ->
                ExpandedEntity(id, type, properties, relationships, geoProperties, this.contexts).validNel()
        })

// TODO can we do better when we are sure we have a valid entity (e.g. on an internal event)?
fun JsonLdExpandedEntity.toKnownValidEntity(): ExpandedEntity =
    this.toEntity().orNull()!!
