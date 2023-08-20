package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.flatten
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.extractRelationshipObject
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsString
import java.net.URI
import java.time.ZonedDateTime

class NgsiLdEntity private constructor(
    val id: URI,
    val types: List<ExpandedTerm>,
    val scopes: List<String>?,
    val relationships: List<NgsiLdRelationship>,
    val properties: List<NgsiLdProperty>,
    val geoProperties: List<NgsiLdGeoProperty>,
    val contexts: List<String>
) {
    companion object {
        suspend fun create(
            parsedKeys: Map<String, Any>,
            contexts: List<String>
        ): Either<APIException, NgsiLdEntity> = either {
            ensure(parsedKeys.containsKey(JSONLD_ID)) {
                BadRequestDataException("The provided NGSI-LD entity does not contain an id property")
            }
            val id = (parsedKeys[JSONLD_ID]!! as String).toUri()

            ensure(parsedKeys.containsKey(JSONLD_TYPE)) {
                BadRequestDataException("The provided NGSI-LD entity does not contain a type property")
            }
            val types = parsedKeys[JSONLD_TYPE]!! as List<String>

            val scopes = (parsedKeys as Map<String, List<Any>>).getScopes()

            val attributes = getNonCoreAttributes(parsedKeys, NGSILD_ENTITY_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE).bind()
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE).bind()
            val geoProperties = getAttributesOfType<NgsiLdGeoProperty>(attributes, NGSILD_GEOPROPERTY_TYPE).bind()
            ensure(attributes.size == relationships.size + properties.size + geoProperties.size) {
                val attributesWithUnknownTypes =
                    attributes.keys.minus(setOf(relationships, properties, geoProperties).flatten().map { it.name })
                BadRequestDataException("Entity has attribute(s) with an unknown type: $attributesWithUnknownTypes")
            }

            NgsiLdEntity(id, types, scopes, relationships, properties, geoProperties, contexts)
        }
    }

    val attributes: List<NgsiLdAttribute> = properties.plus(relationships).plus(geoProperties)
}

sealed class NgsiLdAttribute(val name: String) {
    abstract fun getAttributeInstances(): List<NgsiLdAttributeInstance>
}

class NgsiLdProperty private constructor(
    name: String,
    val instances: List<NgsiLdPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        suspend fun create(
            name: String,
            instances: ExpandedAttributeInstances
        ): Either<APIException, NgsiLdProperty> = either {
            checkInstancesAreOfSameType(name, instances, NGSILD_PROPERTY_TYPE).bind()

            val ngsiLdPropertyInstances = instances.parMap { instance ->
                NgsiLdPropertyInstance.create(name, instance).bind()
            }

            checkAttributeDefaultInstance(name, ngsiLdPropertyInstances).bind()
            checkAttributeDuplicateDatasetId(name, ngsiLdPropertyInstances).bind()

            NgsiLdProperty(name, ngsiLdPropertyInstances)
        }
    }

    override fun getAttributeInstances(): List<NgsiLdPropertyInstance> = instances
}

class NgsiLdRelationship private constructor(
    name: String,
    val instances: List<NgsiLdRelationshipInstance>
) : NgsiLdAttribute(name) {
    companion object {
        suspend fun create(
            name: String,
            instances: ExpandedAttributeInstances
        ): Either<APIException, NgsiLdRelationship> = either {
            checkInstancesAreOfSameType(name, instances, NGSILD_RELATIONSHIP_TYPE).bind()

            val ngsiLdRelationshipInstances = instances.parMap { instance ->
                NgsiLdRelationshipInstance.create(name, instance).bind()
            }

            checkAttributeDefaultInstance(name, ngsiLdRelationshipInstances).bind()
            checkAttributeDuplicateDatasetId(name, ngsiLdRelationshipInstances).bind()

            NgsiLdRelationship(name, ngsiLdRelationshipInstances)
        }
    }

    override fun getAttributeInstances(): List<NgsiLdRelationshipInstance> = instances
}

class NgsiLdGeoProperty private constructor(
    name: String,
    val instances: List<NgsiLdGeoPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        suspend fun create(
            name: String,
            instances: ExpandedAttributeInstances
        ): Either<APIException, NgsiLdGeoProperty> = either {
            checkInstancesAreOfSameType(name, instances, NGSILD_GEOPROPERTY_TYPE).bind()

            val ngsiLdGeoPropertyInstances = instances.parMap { instance ->
                NgsiLdGeoPropertyInstance.create(name, instance).bind()
            }

            checkAttributeDefaultInstance(name, ngsiLdGeoPropertyInstances).bind()
            checkAttributeDuplicateDatasetId(name, ngsiLdGeoPropertyInstances).bind()

            NgsiLdGeoProperty(name, ngsiLdGeoPropertyInstances)
        }
    }

    override fun getAttributeInstances(): List<NgsiLdAttributeInstance> = instances
}

sealed class NgsiLdAttributeInstance(
    val createdAt: ZonedDateTime? = ngsiLdDateTime(),
    val modifiedAt: ZonedDateTime?,
    val observedAt: ZonedDateTime?,
    val datasetId: URI?,
    val properties: List<NgsiLdProperty>,
    val relationships: List<NgsiLdRelationship>
)

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
        suspend fun create(
            name: String,
            values: ExpandedAttributeInstance
        ): Either<APIException, NgsiLdPropertyInstance> = either {
            val value = getPropertyValueFromMap(values, NGSILD_PROPERTY_VALUE)
            ensureNotNull(value) {
                BadRequestDataException("Property $name has an instance without a value")
            }

            val unitCode = getPropertyValueFromMapAsString(values, NGSILD_UNIT_CODE_PROPERTY)
            val createdAt = getPropertyValueFromMapAsDateTime(values, NGSILD_CREATED_AT_PROPERTY)
            val modifiedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_MODIFIED_AT_PROPERTY)
            val observedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val attributes = getNonCoreAttributes(values, NGSILD_PROPERTIES_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE).bind()
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE).bind()
            ensure(attributes.size == relationships.size + properties.size) {
                BadRequestDataException("Property $name has unknown attributes types: $attributes")
            }

            NgsiLdPropertyInstance(
                value,
                unitCode,
                createdAt,
                modifiedAt,
                observedAt,
                datasetId,
                properties,
                relationships
            )
        }
    }

    override fun toString(): String = "NgsiLdPropertyInstance(value=$value)"
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
        suspend fun create(
            name: String,
            values: ExpandedAttributeInstance
        ): Either<APIException, NgsiLdRelationshipInstance> = either {
            val objectId = extractRelationshipObject(name, values).bind()
            val createdAt = getPropertyValueFromMapAsDateTime(values, NGSILD_CREATED_AT_PROPERTY)
            val modifiedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_MODIFIED_AT_PROPERTY)
            val observedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val attributes = getNonCoreAttributes(values, NGSILD_RELATIONSHIPS_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE).bind()
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE).bind()
            ensure(attributes.size == relationships.size + properties.size) {
                BadRequestDataException("Relationship $name has unknown attributes: $attributes")
            }

            NgsiLdRelationshipInstance(
                objectId,
                createdAt,
                modifiedAt,
                observedAt,
                datasetId,
                properties,
                relationships
            )
        }
    }

    override fun toString(): String = "NgsiLdRelationshipInstance(objectId=$objectId)"
}

class NgsiLdGeoPropertyInstance(
    val coordinates: WKTCoordinates,
    createdAt: ZonedDateTime?,
    modifiedAt: ZonedDateTime?,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    properties: List<NgsiLdProperty>,
    relationships: List<NgsiLdRelationship>
) : NgsiLdAttributeInstance(createdAt, modifiedAt, observedAt, datasetId, properties, relationships) {
    companion object {
        suspend fun create(
            name: String,
            values: ExpandedAttributeInstance
        ): Either<APIException, NgsiLdGeoPropertyInstance> = either {
            val createdAt = getPropertyValueFromMapAsDateTime(values, NGSILD_CREATED_AT_PROPERTY)
            val modifiedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_MODIFIED_AT_PROPERTY)
            val observedAt = getPropertyValueFromMapAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val wktValue = (values[NGSILD_GEOPROPERTY_VALUE]!![0] as Map<String, String>)[JSONLD_VALUE] as String
            val attributes = getNonCoreAttributes(values, NGSILD_GEOPROPERTIES_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE).bind()
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE).bind()
            ensure(attributes.size == relationships.size + properties.size) {
                BadRequestDataException("Geoproperty $name has unknown attributes: $attributes")
            }

            NgsiLdGeoPropertyInstance(
                WKTCoordinates(wktValue),
                createdAt,
                modifiedAt,
                observedAt,
                datasetId,
                properties,
                relationships
            )
        }
    }

    override fun toString(): String = "NgsiLdGeoPropertyInstance(coordinates=$coordinates)"
}

@JvmInline
value class WKTCoordinates(val value: String)

/**
 * Given an entity's attribute, returns whether it is of the given attribute type
 * (i.e. property, geo property or relationship)
 */
fun isAttributeOfType(attributeInstance: ExpandedAttributeInstance, type: AttributeType): Boolean =
    attributeInstance.containsKey(JSONLD_TYPE) &&
        attributeInstance[JSONLD_TYPE] is List<*> &&
        attributeInstance.getOrElse(JSONLD_TYPE) { emptyList() }[0] == type.uri

private suspend inline fun <reified T : NgsiLdAttribute> getAttributesOfType(
    attributes: Map<String, Any>,
    type: AttributeType
): Either<APIException, List<T>> = either {
    attributes
        .mapValues {
            JsonLdUtils.castAttributeValue(it.value)
        }.filter {
            // only check the first entry, multi-attribute consistency is later checked by each attribute
            isAttributeOfType(it.value[0], type)
        }.entries
        .map {
            when (type) {
                NGSILD_PROPERTY_TYPE -> NgsiLdProperty.create(it.key, it.value).bind() as T
                NGSILD_RELATIONSHIP_TYPE -> NgsiLdRelationship.create(it.key, it.value).bind() as T
                NGSILD_GEOPROPERTY_TYPE -> NgsiLdGeoProperty.create(it.key, it.value).bind() as T
                else -> BadRequestDataException("Unrecognized type: $type").left().bind<T>()
            }
        }
}

private fun getNonCoreAttributes(parsedKeys: Map<String, Any>, keysToFilter: List<String>): Map<String, Any> =
    parsedKeys.filterKeys {
        !keysToFilter.contains(it)
    }

suspend fun checkInstancesAreOfSameType(
    name: String,
    values: ExpandedAttributeInstances,
    type: AttributeType
): Either<APIException, Unit> = either {
    ensure(values.all { isAttributeOfType(it, type) }) {
        BadRequestDataException("Attribute $name instances must have the same type")
    }
}

suspend fun checkAttributeDefaultInstance(
    name: String,
    instances: List<NgsiLdAttributeInstance>
): Either<APIException, Unit> = either {
    ensure(instances.count { it.datasetId == null } <= 1) {
        BadRequestDataException("Attribute $name can't have more than one default instance")
    }
}

fun checkAttributeDuplicateDatasetId(
    name: String,
    instances: List<NgsiLdAttributeInstance>
): Either<APIException, Unit> {
    val datasetIds = instances.map {
        it.datasetId
    }
    return if (datasetIds.toSet().count() != datasetIds.count())
        BadRequestDataException("Attribute $name can't have more than one instance with the same datasetId").left()
    else Unit.right()
}

suspend fun ExpandedAttributes.toNgsiLdAttributes(): Either<APIException, List<NgsiLdAttribute>> = either {
    entries.parMap {
        it.value.toNgsiLdAttribute(it.key).bind()
    }
}

suspend fun ExpandedAttribute.toNgsiLdAttribute(): Either<APIException, NgsiLdAttribute> =
    this.second.toNgsiLdAttribute(this.first)

suspend fun ExpandedAttributeInstances.toNgsiLdAttribute(
    attributeName: ExpandedTerm
): Either<APIException, NgsiLdAttribute> =
    when {
        isAttributeOfType(this[0], NGSILD_PROPERTY_TYPE) ->
            NgsiLdProperty.create(attributeName, this)
        isAttributeOfType(this[0], NGSILD_RELATIONSHIP_TYPE) ->
            NgsiLdRelationship.create(attributeName, this)
        isAttributeOfType(this[0], NGSILD_GEOPROPERTY_TYPE) ->
            NgsiLdGeoProperty.create(attributeName, this)
        else -> BadRequestDataException("Unrecognized type for $attributeName").left()
    }

suspend fun JsonLdEntity.toNgsiLdEntity(): Either<APIException, NgsiLdEntity> =
    NgsiLdEntity.create(this.members, this.contexts)

fun ExpandedAttributeInstance.getDatasetId(): URI? =
    (this[NGSILD_DATASET_ID_PROPERTY]?.get(0) as? Map<String, String>)?.get(JSONLD_ID)?.toUri()

fun ExpandedAttributeInstance.getScopes(): List<String>? =
    when (val rawScopes = getPropertyValueFromMap(this, NGSILD_SCOPE_PROPERTY)) {
        is String -> listOf(rawScopes)
        is List<*> -> rawScopes as List<String>
        else -> null
    }

fun ExpandedAttributeInstance.getPropertyValue(): Any =
    (this[NGSILD_PROPERTY_VALUE]!![0] as Map<String, Any>)[JSONLD_VALUE]!!

fun List<NgsiLdAttribute>.flatOnInstances(): List<Pair<NgsiLdAttribute, NgsiLdAttributeInstance>> =
    this.flatMap { ngsiLdAttribute ->
        ngsiLdAttribute.getAttributeInstances().map { Pair(ngsiLdAttribute, it) }
    }

val NGSILD_ENTITY_CORE_MEMBERS = listOf(
    JSONLD_ID,
    JSONLD_TYPE,
    NGSILD_SCOPE_PROPERTY,
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
