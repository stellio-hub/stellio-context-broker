package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.AttributeType
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_COORDINATES_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandValueAsMap
import com.egm.stellio.shared.util.JsonLdUtils.extractRelationshipObject
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsString
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.time.ZonedDateTime
import java.util.Locale

class NgsiLdEntity private constructor(
    val id: URI,
    val type: String,
    val relationships: List<NgsiLdRelationship>,
    val properties: List<NgsiLdProperty>,
    // TODO by 5.2.4, it is at most one location, one observationSpace and one operationSpace (to be enforced)
    //      but nothing prevents to add other user-defined geo properties
    val geoProperties: List<NgsiLdGeoProperty>,
    val contexts: List<String>
) {
    companion object {
        operator fun invoke(parsedKeys: Map<String, Any>, contexts: List<String>): NgsiLdEntity {
            if (!parsedKeys.containsKey(JSONLD_ID))
                throw BadRequestDataException("The provided NGSI-LD entity does not contain an id property")
            val id = (parsedKeys[JSONLD_ID]!! as String).toUri()

            if (!parsedKeys.containsKey(JSONLD_TYPE))
                throw BadRequestDataException("The provided NGSI-LD entity does not contain a type property")
            val type = (parsedKeys[JSONLD_TYPE]!! as List<String>)[0]
            if (!type.extractShortTypeFromExpanded().isNgsiLdSupportedName())
                throw BadRequestDataException("The provided NGSI-LD entity has a type with invalid characters")

            val attributes = getNonCoreAttributes(parsedKeys, NGSILD_ENTITY_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE)
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE)
            val geoProperties = getAttributesOfType<NgsiLdGeoProperty>(attributes, NGSILD_GEOPROPERTY_TYPE)
            if (attributes.size > relationships.size + properties.size + geoProperties.size)
                throw BadRequestDataException("Entity has unknown attributes types: $attributes")

            return NgsiLdEntity(id, type, relationships, properties, geoProperties, contexts)
        }
    }

    val attributes: List<NgsiLdAttribute> = properties.plus(relationships).plus(geoProperties)

    /**
     * Gets linked entities ids.
     * Entities can be linked either by a relation or a property.
     */
    fun getLinkedEntitiesIds(): List<URI> =
        properties.flatMap {
            it.getLinkedEntitiesIds()
        }.plus(
            relationships.flatMap { it.getLinkedEntitiesIds() }
        ).plus(
            geoProperties.flatMap { it.getLinkedEntitiesIds() }
        )

    fun getLocation(): NgsiLdGeoProperty? =
        geoProperties.find { it.name == NGSILD_LOCATION_PROPERTY }
}

sealed class NgsiLdAttribute(val name: String) {
    val compactName: String = name.extractShortTypeFromExpanded()

    init {
        if (!compactName.isNgsiLdSupportedName())
            throw BadRequestDataException("Entity has an invalid attribute name: $compactName")
    }

    abstract fun getLinkedEntitiesIds(): List<URI>
    abstract fun getAttributeInstances(): List<NgsiLdAttributeInstance>
}

class NgsiLdProperty private constructor(
    name: String,
    val instances: List<NgsiLdPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        operator fun invoke(name: String, instances: List<Map<String, List<Any>>>): NgsiLdProperty {
            checkInstancesAreOfSameType(name, instances, NGSILD_PROPERTY_TYPE)

            val ngsiLdPropertyInstances = instances.map { instance ->
                NgsiLdPropertyInstance(name, instance)
            }

            checkAttributeDefaultInstance(name, ngsiLdPropertyInstances)
            checkAttributeDuplicateDatasetId(name, ngsiLdPropertyInstances)

            return NgsiLdProperty(name, ngsiLdPropertyInstances)
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        instances.flatMap { it.getLinkedEntitiesIds() }

    override fun getAttributeInstances(): List<NgsiLdPropertyInstance> = instances
}

class NgsiLdRelationship private constructor(
    name: String,
    val instances: List<NgsiLdRelationshipInstance>
) : NgsiLdAttribute(name) {
    companion object {
        operator fun invoke(name: String, instances: List<Map<String, List<Any>>>): NgsiLdRelationship {
            checkInstancesAreOfSameType(name, instances, NGSILD_RELATIONSHIP_TYPE)

            val ngsiLdRelationshipInstances = instances.map { instance ->
                NgsiLdRelationshipInstance(name, instance)
            }

            checkAttributeDefaultInstance(name, ngsiLdRelationshipInstances)
            checkAttributeDuplicateDatasetId(name, ngsiLdRelationshipInstances)

            return NgsiLdRelationship(name, ngsiLdRelationshipInstances)
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        instances.flatMap { it.getLinkedEntitiesIds() }

    override fun getAttributeInstances(): List<NgsiLdRelationshipInstance> = instances
}

class NgsiLdGeoProperty private constructor(
    name: String,
    val instances: List<NgsiLdGeoPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        operator fun invoke(name: String, instances: List<Map<String, List<Any>>>): NgsiLdGeoProperty {
            checkInstancesAreOfSameType(name, instances, NGSILD_GEOPROPERTY_TYPE)

            val ngsiLdGeoPropertyInstances = instances.map { instance ->
                NgsiLdGeoPropertyInstance(instance)
            }

            checkAttributeDefaultInstance(name, ngsiLdGeoPropertyInstances)
            checkAttributeDuplicateDatasetId(name, ngsiLdGeoPropertyInstances)

            return NgsiLdGeoProperty(name, ngsiLdGeoPropertyInstances)
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        instances.flatMap { it.getLinkedEntitiesIds() }

    override fun getAttributeInstances(): List<NgsiLdAttributeInstance> = instances
}

sealed class NgsiLdAttributeInstance(
    val createdAt: ZonedDateTime?,
    val modifiedAt: ZonedDateTime?,
    val observedAt: ZonedDateTime?,
    val datasetId: URI?,
    val properties: List<NgsiLdProperty>,
    val relationships: List<NgsiLdRelationship>
) {
    open fun getLinkedEntitiesIds(): List<URI> =
        properties.flatMap {
            it.getLinkedEntitiesIds()
        }
}

class NgsiLdPropertyInstance private constructor(
    val value: Any,
    val unitCode: String?,
    createdAt: ZonedDateTime?,
    modifiedAt: ZonedDateTime?,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    properties: List<NgsiLdProperty>,
    relationships: List<NgsiLdRelationship>
) : NgsiLdAttributeInstance(createdAt, modifiedAt, observedAt, datasetId, properties, relationships) {
    companion object {
        operator fun invoke(name: String, values: Map<String, List<Any>>): NgsiLdPropertyInstance {
            // TODO for short-handed properties, the value is directly accessible from the map under the @value key ?
            val value = getPropertyValueFromMap(values, NGSILD_PROPERTY_VALUE)
                ?: throw BadRequestDataException("Property $name has an instance without a value")

            val unitCode = getPropertyValueFromMapAsString(values, NGSILD_UNIT_CODE_PROPERTY)
            val createdAt = getPropertyValueFromMapAsDateTime(values, NGSILD_CREATED_AT_PROPERTY)
            val modifiedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_MODIFIED_AT_PROPERTY)
            val observedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val attributes = getNonCoreAttributes(values, NGSILD_PROPERTIES_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE)
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE)
            if (attributes.size > relationships.size + properties.size)
                throw BadRequestDataException("Property has unknown attributes types: $attributes")

            return NgsiLdPropertyInstance(
                value, unitCode, createdAt, modifiedAt, observedAt, datasetId, properties, relationships
            )
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        relationships.flatMap { it.getLinkedEntitiesIds() }
}

class NgsiLdRelationshipInstance private constructor(
    val objectId: URI,
    createdAt: ZonedDateTime?,
    modifiedAt: ZonedDateTime?,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    properties: List<NgsiLdProperty>,
    relationships: List<NgsiLdRelationship>
) : NgsiLdAttributeInstance(createdAt, modifiedAt, observedAt, datasetId, properties, relationships) {
    companion object {
        operator fun invoke(name: String, values: Map<String, List<Any>>): NgsiLdRelationshipInstance {
            val objectId = extractRelationshipObject(name, values).fold({ throw it }, { it })
            val createdAt = getPropertyValueFromMapAsDateTime(values, NGSILD_CREATED_AT_PROPERTY)
            val modifiedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_MODIFIED_AT_PROPERTY)
            val observedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val attributes = getNonCoreAttributes(values, NGSILD_RELATIONSHIPS_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE)
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE)
            if (attributes.size > relationships.size + properties.size)
                throw BadRequestDataException("Relationship has unknown attributes: $attributes")

            return NgsiLdRelationshipInstance(
                objectId, createdAt, modifiedAt, observedAt, datasetId, properties, relationships
            )
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        relationships.flatMap { it.getLinkedEntitiesIds() }.plus(objectId)
}

class NgsiLdGeoPropertyInstance(
    val geoPropertyType: GeoPropertyType,
    val coordinates: List<Any>,
    createdAt: ZonedDateTime?,
    modifiedAt: ZonedDateTime?,
    observedAt: ZonedDateTime? = null,
    datasetId: URI? = null,
    properties: List<NgsiLdProperty> = emptyList(),
    relationships: List<NgsiLdRelationship> = emptyList()
) : NgsiLdAttributeInstance(createdAt, modifiedAt, observedAt, datasetId, properties, relationships) {
    companion object {
        operator fun invoke(values: Map<String, List<Any>>): NgsiLdGeoPropertyInstance {
            val createdAt = getPropertyValueFromMapAsDateTime(values, NGSILD_CREATED_AT_PROPERTY)
            val modifiedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_MODIFIED_AT_PROPERTY)
            val observedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val geoPropertyValue = expandValueAsMap(values[NGSILD_GEOPROPERTY_VALUE]!!)
            val geoPropertyType = GeoPropertyType.valueOf(
                (geoPropertyValue["@type"]!![0] as String).extractShortTypeFromExpanded()
            )
            val coordinates = extractCoordinates(geoPropertyType, geoPropertyValue)

            val attributes = getNonCoreAttributes(values, NGSILD_GEOPROPERTIES_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE)
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE)
            if (attributes.size > relationships.size + properties.size)
                throw BadRequestDataException("Geoproperty has unknown attributes: $attributes")

            return NgsiLdGeoPropertyInstance(
                geoPropertyType, coordinates, createdAt, modifiedAt, observedAt, datasetId, properties, relationships
            )
        }

        // TODO this lacks sanity checks
        private fun extractCoordinates(geoPropertyType: GeoPropertyType, geoPropertyValue: Map<String, List<Any>>):
            List<Any> {
            val coordinates = geoPropertyValue[NGSILD_COORDINATES_PROPERTY]!!
            if (geoPropertyType == GeoPropertyType.Point) {
                val longitude = (coordinates[0] as Map<String, Double>)["@value"]!!
                val latitude = (coordinates[1] as Map<String, Double>)["@value"]!!
                return listOf(longitude, latitude)
            } else {
                val res = arrayListOf<List<Double?>>()
                var count = 1
                coordinates.forEach {
                    if (count % 2 != 0) {
                        val longitude = (coordinates[count - 1] as Map<String, Double>)["@value"]!!
                        val latitude = (coordinates[count] as Map<String, Double>)["@value"]!!
                        res.add(listOf(longitude, latitude))
                    }
                    count++
                }
                return res
            }
        }

        fun toWktFormat(geoPropertyType: GeoPropertyType, coordinates: List<Any>): String {
            return if (geoPropertyType == GeoPropertyType.Point) {
                "${geoPropertyType.value.uppercase(Locale.getDefault())} (${coordinates.joinToString(" ")})"
            } else {
                val formattedCoordinates = coordinates.map {
                    it as List<*>
                    it.joinToString(" ")
                }
                "${geoPropertyType.value.uppercase(Locale.getDefault())} ((${formattedCoordinates.joinToString(", ")}))"
            }
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        relationships.flatMap { it.getLinkedEntitiesIds() }
}

/**
 * Given an entity's attribute, returns whether it is of the given attribute type
 * (i.e. property, geo property or relationship)
 */
fun isAttributeOfType(values: Map<String, List<Any>>, type: AttributeType): Boolean =
    // TODO move some of these checks to isValidAttribute()
    values.containsKey(JSONLD_TYPE) &&
        values[JSONLD_TYPE] is List<*> &&
        values.getOrElse(JSONLD_TYPE) { emptyList() }[0] == type.uri

private inline fun <reified T : NgsiLdAttribute> getAttributesOfType(
    attributes: Map<String, Any>,
    type: AttributeType
): List<T> =
    attributes.mapValues {
        JsonLdUtils.expandValueAsListOfMap(it.value)
    }.filter {
        // only check the first entry, multi-attribute consistency is later checked by each attribute
        isAttributeOfType(it.value[0], type)
    }.map {
        when (type) {
            NGSILD_PROPERTY_TYPE -> NgsiLdProperty(it.key, it.value) as T
            NGSILD_RELATIONSHIP_TYPE -> NgsiLdRelationship(it.key, it.value) as T
            NGSILD_GEOPROPERTY_TYPE -> NgsiLdGeoProperty(it.key, it.value) as T
            else -> throw BadRequestDataException("Unrecognized type: $type")
        }
    }

private fun getNonCoreAttributes(parsedKeys: Map<String, Any>, keysToFilter: List<String>): Map<String, Any> =
    parsedKeys.filterKeys {
        !keysToFilter.contains(it)
    }

// TODO to be refactored with validation
fun checkInstancesAreOfSameType(name: String, values: List<Map<String, List<Any>>>, type: AttributeType) {
    if (!values.all { isAttributeOfType(it, type) })
        throw BadRequestDataException("Attribute $name instances must have the same type")
}

// TODO to be refactored with validation
fun checkAttributeDefaultInstance(name: String, instances: List<NgsiLdAttributeInstance>) {
    if (instances.count { it.datasetId == null } > 1)
        throw BadRequestDataException("Attribute $name can't have more than one default instance")
}

// TODO to be refactored with validation
fun checkAttributeDuplicateDatasetId(name: String, instances: List<NgsiLdAttributeInstance>) {
    val datasetIds = instances.map {
        it.datasetId
    }
    if (datasetIds.toSet().count() != datasetIds.count())
        throw BadRequestDataException("Attribute $name can't have more than one instance with the same datasetId")
}

fun parseToNgsiLdAttributes(attributes: Map<String, Any>): List<NgsiLdAttribute> =
    attributes.mapValues {
        JsonLdUtils.expandValueAsListOfMap(it.value)
    }.map {
        when {
            isAttributeOfType(it.value[0], NGSILD_PROPERTY_TYPE) -> NgsiLdProperty(it.key, it.value)
            isAttributeOfType(it.value[0], NGSILD_RELATIONSHIP_TYPE) -> NgsiLdRelationship(it.key, it.value)
            isAttributeOfType(it.value[0], NGSILD_GEOPROPERTY_TYPE) -> NgsiLdGeoProperty(it.key, it.value)
            else -> throw BadRequestDataException("Unrecognized type for ${it.key}")
        }
    }

fun String.isNgsiLdSupportedName() =
    this.all { char -> char.isLetterOrDigit() || listOf(':', '_').contains(char) }

fun JsonLdEntity.toNgsiLdEntity(): NgsiLdEntity =
    NgsiLdEntity(this.properties, this.contexts)

fun Map<String, List<Any>>.getDatasetId(): URI? =
    (this[NGSILD_DATASET_ID_PROPERTY]?.get(0) as Map<String, String>?)?.get(JSONLD_ID)?.toUri()

val NGSILD_ENTITY_CORE_MEMBERS = listOf(
    JSONLD_ID,
    JSONLD_TYPE,
    NGSILD_CREATED_AT_PROPERTY,
    NGSILD_MODIFIED_AT_PROPERTY
)

val NGSILD_ATTRIBUTES_CORE_MEMBERS = listOf(
    JSONLD_TYPE,
    NGSILD_CREATED_AT_PROPERTY,
    NGSILD_MODIFIED_AT_PROPERTY,
    NGSILD_OBSERVED_AT_PROPERTY,
    NGSILD_DATASET_ID_PROPERTY
)

val NGSILD_PROPERTIES_CORE_MEMBERS = listOf(
    NGSILD_PROPERTY_VALUE,
    NGSILD_UNIT_CODE_PROPERTY
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

val NGSILD_RELATIONSHIPS_CORE_MEMBERS = listOf(
    NGSILD_RELATIONSHIP_HAS_OBJECT
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

val NGSILD_GEOPROPERTIES_CORE_MEMBERS = listOf(
    NGSILD_GEOPROPERTY_VALUE
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

enum class GeoPropertyType(val value: String) {
    Point("Point"),
    Polygon("Polygon")
}

// basic alias to help identify, mainly in method calls, if the expected value is a compact or expanded one
typealias ExpandedTerm = String
