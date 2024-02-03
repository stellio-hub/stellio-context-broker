package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.shared.util.AttributeType
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.time.ZonedDateTime

class NgsiLdEntity private constructor(
    val id: URI,
    val types: List<ExpandedTerm>,
    val scopes: List<String>?,
    val attributes: List<NgsiLdAttribute>
) {
    companion object {
        suspend fun create(
            parsedKeys: Map<String, Any>
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

            val rawAttributes = getNonCoreMembers(parsedKeys, NGSILD_ENTITY_CORE_MEMBERS)
            val attributes = parseAttributes(rawAttributes).bind()

            NgsiLdEntity(id, types, scopes, attributes)
        }
    }

    inline fun <reified T : NgsiLdAttribute> getAttributesOfType(): List<T> = attributes.filterIsInstance<T>()

    val properties = getAttributesOfType<NgsiLdProperty>()
    val relationships = getAttributesOfType<NgsiLdRelationship>()
    val geoProperties = getAttributesOfType<NgsiLdGeoProperty>()
    val jsonProperties = getAttributesOfType<NgsiLdJsonProperty>()
}

sealed class NgsiLdAttribute(val name: ExpandedTerm) {
    abstract fun getAttributeInstances(): List<NgsiLdAttributeInstance>
}

class NgsiLdProperty private constructor(
    name: ExpandedTerm,
    val instances: List<NgsiLdPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        suspend fun create(
            name: ExpandedTerm,
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
    name: ExpandedTerm,
    val instances: List<NgsiLdRelationshipInstance>
) : NgsiLdAttribute(name) {
    companion object {
        suspend fun create(
            name: ExpandedTerm,
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
    name: ExpandedTerm,
    val instances: List<NgsiLdGeoPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        suspend fun create(
            name: ExpandedTerm,
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

class NgsiLdJsonProperty private constructor(
    name: ExpandedTerm,
    val instances: List<NgsiLdJsonPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        suspend fun create(
            name: ExpandedTerm,
            instances: ExpandedAttributeInstances
        ): Either<APIException, NgsiLdJsonProperty> = either {
            checkInstancesAreOfSameType(name, instances, NGSILD_JSONPROPERTY_TYPE).bind()

            val ngsiLdJsonPropertyInstances = instances.parMap { instance ->
                NgsiLdJsonPropertyInstance.create(name, instance).bind()
            }

            checkAttributeDefaultInstance(name, ngsiLdJsonPropertyInstances).bind()
            checkAttributeDuplicateDatasetId(name, ngsiLdJsonPropertyInstances).bind()

            NgsiLdJsonProperty(name, ngsiLdJsonPropertyInstances)
        }
    }

    override fun getAttributeInstances(): List<NgsiLdJsonPropertyInstance> = instances
}

sealed class NgsiLdAttributeInstance(
    val observedAt: ZonedDateTime?,
    val datasetId: URI?,
    val attributes: List<NgsiLdAttribute>
)

class NgsiLdPropertyInstance private constructor(
    val value: Any,
    val unitCode: String?,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    attributes: List<NgsiLdAttribute>
) : NgsiLdAttributeInstance(observedAt, datasetId, attributes) {
    companion object {
        suspend fun create(
            name: ExpandedTerm,
            values: ExpandedAttributeInstance
        ): Either<APIException, NgsiLdPropertyInstance> = either {
            val value = values.getPropertyValue()
            ensureNotNull(value) {
                BadRequestDataException("Property $name has an instance without a value")
            }

            val unitCode = values.getMemberValueAsString(NGSILD_UNIT_CODE_PROPERTY)
            val observedAt = values.getMemberValueAsDateTime(NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            checkAttributeHasNoForbiddenMembers(name, values, NGSILD_PROPERTIES_FORBIDDEN_MEMBERS).bind()

            val rawAttributes = getNonCoreMembers(values, NGSILD_PROPERTIES_CORE_MEMBERS)
            val attributes = parseAttributes(rawAttributes).bind()

            NgsiLdPropertyInstance(
                value,
                unitCode,
                observedAt,
                datasetId,
                attributes
            )
        }
    }

    override fun toString(): String = "NgsiLdPropertyInstance(value=$value)"
}

class NgsiLdRelationshipInstance private constructor(
    val objectId: URI,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    attributes: List<NgsiLdAttribute>
) : NgsiLdAttributeInstance(observedAt, datasetId, attributes) {
    companion object {
        suspend fun create(
            name: ExpandedTerm,
            values: ExpandedAttributeInstance
        ): Either<APIException, NgsiLdRelationshipInstance> = either {
            val objectId = values.getRelationshipObject(name).bind()
            val observedAt = values.getMemberValueAsDateTime(NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            checkAttributeHasNoForbiddenMembers(name, values, NGSILD_RELATIONSHIPS_FORBIDDEN_MEMBERS).bind()

            val rawAttributes = getNonCoreMembers(values, NGSILD_RELATIONSHIPS_CORE_MEMBERS)
            val attributes = parseAttributes(rawAttributes).bind()

            NgsiLdRelationshipInstance(
                objectId,
                observedAt,
                datasetId,
                attributes
            )
        }
    }

    override fun toString(): String = "NgsiLdRelationshipInstance(objectId=$objectId)"
}

class NgsiLdGeoPropertyInstance(
    val coordinates: WKTCoordinates,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    attributes: List<NgsiLdAttribute>
) : NgsiLdAttributeInstance(observedAt, datasetId, attributes) {
    companion object {
        suspend fun create(
            name: ExpandedTerm,
            values: ExpandedAttributeInstance
        ): Either<APIException, NgsiLdGeoPropertyInstance> = either {
            val wktValue = values.getMemberValue(NGSILD_GEOPROPERTY_VALUE) as? String
            ensureNotNull(wktValue) {
                BadRequestDataException("GeoProperty $name has an instance without a value")
            }
            val observedAt = values.getMemberValueAsDateTime(NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            checkAttributeHasNoForbiddenMembers(name, values, NGSILD_GEOPROPERTIES_FORBIDDEN_MEMBERS).bind()

            val rawAttributes = getNonCoreMembers(values, NGSILD_GEOPROPERTIES_CORE_MEMBERS)
            val attributes = parseAttributes(rawAttributes).bind()

            NgsiLdGeoPropertyInstance(
                WKTCoordinates(wktValue),
                observedAt,
                datasetId,
                attributes
            )
        }
    }

    override fun toString(): String = "NgsiLdGeoPropertyInstance(coordinates=$coordinates)"
}

class NgsiLdJsonPropertyInstance private constructor(
    val json: Any,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    attributes: List<NgsiLdAttribute>
) : NgsiLdAttributeInstance(observedAt, datasetId, attributes) {
    companion object {
        suspend fun create(
            name: ExpandedTerm,
            values: ExpandedAttributeInstance
        ): Either<APIException, NgsiLdJsonPropertyInstance> = either {
            val json = values.getMemberValue(NGSILD_JSONPROPERTY_VALUE)
            ensureNotNull(json) {
                BadRequestDataException("Property $name has an instance without a json member")
            }
            ensure(json is Map<*, *> || (json is List<*> && json.all { it is Map<*, *> })) {
                BadRequestDataException(
                    "Property $name has a json member that is not a JSON object, nor an array of JSON objects"
                )
            }

            val observedAt = values.getMemberValueAsDateTime(NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            checkAttributeHasNoForbiddenMembers(name, values, NGSILD_JSONPROPERTIES_FORBIDDEN_MEMBERS).bind()

            val rawAttributes = getNonCoreMembers(values, NGSILD_JSONPROPERTIES_CORE_MEMBERS)
            val attributes = parseAttributes(rawAttributes).bind()

            NgsiLdJsonPropertyInstance(
                json,
                observedAt,
                datasetId,
                attributes
            )
        }
    }

    override fun toString(): String = "NgsiLdJsonPropertyInstance(json=$json)"
}

@JvmInline
value class WKTCoordinates(val value: String)

/**
 * Given an entity's attribute, returns whether it is of the given attribute type
 * (i.e. property, geo property, json property or relationship)
 */
fun isAttributeOfType(attributeInstance: ExpandedAttributeInstance, type: AttributeType): Boolean =
    attributeInstance.containsKey(JSONLD_TYPE) &&
        attributeInstance[JSONLD_TYPE] is List<*> &&
        attributeInstance.getOrElse(JSONLD_TYPE) { emptyList() }[0] == type.uri

private suspend fun parseAttributes(
    attributes: Map<String, Any>
): Either<APIException, List<NgsiLdAttribute>> =
    attributes
        .mapValues { castAttributeValue(it.value) }
        .toList()
        .map {
            val attributeType = (it.second[0][JSONLD_TYPE] as? List<String>)?.get(0)
            when (attributeType) {
                NGSILD_PROPERTY_TYPE.uri -> NgsiLdProperty.create(it.first, it.second)
                NGSILD_RELATIONSHIP_TYPE.uri -> NgsiLdRelationship.create(it.first, it.second)
                NGSILD_GEOPROPERTY_TYPE.uri -> NgsiLdGeoProperty.create(it.first, it.second)
                NGSILD_JSONPROPERTY_TYPE.uri -> NgsiLdJsonProperty.create(it.first, it.second)
                else -> BadRequestDataException("Attribute ${it.first} has an unknown type: $attributeType").left()
            }
        }.let { l ->
            either { l.bindAll() }
        }

private fun getNonCoreMembers(parsedKeys: Map<String, Any>, keysToFilter: List<String>): Map<String, Any> =
    parsedKeys.filterKeys {
        !keysToFilter.contains(it)
    }

fun checkInstancesAreOfSameType(
    name: String,
    values: ExpandedAttributeInstances,
    type: AttributeType
): Either<APIException, Unit> = either {
    ensure(values.all { isAttributeOfType(it, type) }) {
        BadRequestDataException("Attribute $name can't have instances with different types")
    }
}

fun checkAttributeDefaultInstance(
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
): Either<APIException, Unit> = either {
    val datasetIds = instances.map {
        it.datasetId
    }
    ensure(datasetIds.toSet().count() == datasetIds.count()) {
        BadRequestDataException("Attribute $name can't have more than one instance with the same datasetId")
    }
}

fun checkAttributeHasNoForbiddenMembers(
    name: ExpandedTerm,
    instance: ExpandedAttributeInstance,
    forbiddenMembers: List<ExpandedTerm>
): Either<APIException, Unit> = either {
    forbiddenMembers.find {
        instance.getMemberValue(it) != null
    }.let {
        if (it != null) BadRequestDataException("Attribute $name has an instance with a forbidden member: $it").left()
        else Unit.right()
    }
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
        isAttributeOfType(this[0], NGSILD_JSONPROPERTY_TYPE) ->
            NgsiLdJsonProperty.create(attributeName, this)
        else -> BadRequestDataException("Unrecognized type for $attributeName").left()
    }

suspend fun ExpandedEntity.toNgsiLdEntity(): Either<APIException, NgsiLdEntity> =
    NgsiLdEntity.create(this.members)

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

val NGSILD_PROPERTIES_FORBIDDEN_MEMBERS = listOf(
    NGSILD_RELATIONSHIP_OBJECT,
    NGSILD_JSONPROPERTY_VALUE,
)

val NGSILD_RELATIONSHIPS_CORE_MEMBERS = listOf(
    NGSILD_RELATIONSHIP_OBJECT
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

val NGSILD_RELATIONSHIPS_FORBIDDEN_MEMBERS = listOf(
    NGSILD_PROPERTY_VALUE,
    NGSILD_JSONPROPERTY_VALUE,
    NGSILD_UNIT_CODE_PROPERTY
)

val NGSILD_GEOPROPERTIES_CORE_MEMBERS = listOf(
    NGSILD_GEOPROPERTY_VALUE
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

val NGSILD_GEOPROPERTIES_FORBIDDEN_MEMBERS = listOf(
    NGSILD_RELATIONSHIP_OBJECT,
    NGSILD_JSONPROPERTY_VALUE,
    NGSILD_UNIT_CODE_PROPERTY
)

val NGSILD_JSONPROPERTIES_CORE_MEMBERS = listOf(
    NGSILD_JSONPROPERTY_VALUE
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

val NGSILD_JSONPROPERTIES_FORBIDDEN_MEMBERS = listOf(
    NGSILD_RELATIONSHIP_OBJECT,
    NGSILD_PROPERTY_VALUE,
    NGSILD_UNIT_CODE_PROPERTY
)
