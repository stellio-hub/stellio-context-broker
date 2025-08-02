package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import kotlin.reflect.full.safeCast

// basic alias to help identify, mainly in method calls, if the expected value is a compact or expanded one
typealias ExpandedTerm = String
typealias ExpandedAttributes = Map<ExpandedTerm, ExpandedAttributeInstances>
typealias ExpandedAttribute = Pair<ExpandedTerm, ExpandedAttributeInstances>
typealias ExpandedAttributeInstances = List<ExpandedAttributeInstance>
typealias ExpandedAttributeInstance = Map<String, List<Any>>
typealias ExpandedNonReifiedPropertyValue = List<Map<String, Any>>

fun ExpandedAttributes.addCoreMembers(
    entityId: URI,
    entityTypes: List<ExpandedTerm>
): Map<String, Any> =
    this.plus(listOf(JSONLD_ID_KW to entityId, JSONLD_TYPE_KW to entityTypes))

fun ExpandedAttributes.getAttributeFromExpandedAttributes(
    expandedAttributeName: ExpandedTerm,
    datasetId: URI?
): ExpandedAttributeInstance? =
    this[expandedAttributeName]?.let { expandedAttributeInstances ->
        expandedAttributeInstances.find { expandedAttributeInstance ->
            if (datasetId == null)
                !expandedAttributeInstance.containsKey(NGSILD_DATASET_ID_IRI)
            else
                expandedAttributeInstance.getDatasetId() == datasetId
        }
    }

fun ExpandedAttributes.flattenOnAttributeAndDatasetId(): List<Triple<ExpandedTerm, URI?, ExpandedAttributeInstance>> =
    this.flatMap { (attributeName, expandedAttributeInstances) ->
        expandedAttributeInstances.map { expandedAttributeInstance ->
            Triple(attributeName, expandedAttributeInstance.getDatasetId(), expandedAttributeInstance)
        }
    }

fun ExpandedAttribute.toExpandedAttributes(): ExpandedAttributes =
    mapOf(this)

fun ExpandedAttributeInstances.addSubAttribute(
    subAttributeName: ExpandedTerm,
    subAttributePayload: ExpandedAttributeInstances
): ExpandedAttributeInstances {
    if (this.isEmpty() || this.size > 1)
        throw BadRequestDataException("Cannot add a sub-attribute into empty or multi-instance attribute: $this")
    return listOf(this[0].plus(subAttributeName to subAttributePayload))
}

fun ExpandedAttributeInstances.addNonReifiedProperty(
    subAttributeName: ExpandedTerm,
    subAttributeValue: String
): ExpandedAttributeInstances {
    if (this.isEmpty() || this.size > 1)
        throw BadRequestDataException("Cannot add a sub-attribute into empty or multi-instance attribute: $this")
    return listOf(this[0].plus(subAttributeName to buildNonReifiedPropertyValue(subAttributeValue)))
}

fun ExpandedAttributeInstances.addNonReifiedTemporalProperty(
    subAttributeName: ExpandedTerm,
    subAttributeValue: ZonedDateTime
): ExpandedAttributeInstances {
    if (this.isEmpty() || this.size > 1)
        throw BadRequestDataException("Cannot add a sub-attribute into empty or multi-instance attribute: $this")
    return listOf(this[0].plus(subAttributeName to buildNonReifiedTemporalValue(subAttributeValue)))
}

fun ExpandedAttributeInstances.getSingleEntry(): ExpandedAttributeInstance {
    if (this.isEmpty() || this.size > 1)
        throw BadRequestDataException("Expected a single entry but got none or more than one: $this")
    return this[0]
}

fun ExpandedAttributeInstance.addSysAttrs(
    withSysAttrs: Boolean,
    createdAt: ZonedDateTime,
    modifiedAt: ZonedDateTime? = null,
    deletedAt: ZonedDateTime? = null
): ExpandedAttributeInstance =
    if (withSysAttrs)
        this.plus(NGSILD_CREATED_AT_IRI to buildNonReifiedTemporalValue(createdAt))
            .let {
                if (modifiedAt != null)
                    it.plus(NGSILD_MODIFIED_AT_IRI to buildNonReifiedTemporalValue(modifiedAt))
                else it
            }
            .let {
                if (deletedAt != null)
                    it.plus(NGSILD_DELETED_AT_IRI to buildNonReifiedTemporalValue(deletedAt))
                else it
            }
    else this

/**
 * Extract the actual value (@value) of a member from an expanded property.
 *
 * Called on a similar structure:
 * {
 *   https://uri.etsi.org/ngsi-ld/hasValue=[{
 *     @type=[https://uri.etsi.org/ngsi-ld/Property],
 *     @value=250
 *   }],
 *   https://uri.etsi.org/ngsi-ld/unitCode=[{
 *     @value=kg
 *   }],
 *   https://uri.etsi.org/ngsi-ld/observedAt=[{
 *     @value=2019-12-18T10:45:44.248755Z
 *   }]
 * }
 *
 * @return the actual value, e.g. "kg" if provided #memberName is https://uri.etsi.org/ngsi-ld/unitCode
 */
fun ExpandedAttributeInstance.getMemberValue(memberName: ExpandedTerm): Any? {
    if (this[memberName] == null)
        return null

    val intermediateList = this[memberName] as List<Map<String, Any>>
    return if (intermediateList.size == 1) {
        val firstListEntry = intermediateList[0]
        val finalValueType = firstListEntry[JSONLD_TYPE_KW]
        when {
            finalValueType != null -> {
                val finalValue = String::class.safeCast(firstListEntry[JSONLD_VALUE_KW])
                when (finalValueType) {
                    NGSILD_DATE_TIME_TYPE -> ZonedDateTime.parse(finalValue)
                    NGSILD_DATE_TYPE -> LocalDate.parse(finalValue)
                    NGSILD_TIME_TYPE -> LocalTime.parse(finalValue)
                    else -> firstListEntry[JSONLD_VALUE_KW]
                }
            }

            firstListEntry[JSONLD_VALUE_KW] != null ->
                firstListEntry[JSONLD_VALUE_KW]

            firstListEntry[JSONLD_ID_KW] != null -> {
                // Used to get the value of datasetId property,
                // since it is mapped to "@id" key rather than "@value"
                firstListEntry[JSONLD_ID_KW]
            }

            else -> {
                // it is a map / JSON object, keep it as is
                // {https://uri.etsi.org/ngsi-ld/default-context/key=[{@value=value}], ...}
                firstListEntry
            }
        }
    } else {
        intermediateList.map {
            it[JSONLD_VALUE_KW]
        }
    }
}

fun ExpandedAttributeInstance.getAttributeValue(): Pair<String, Any> =
    when {
        this[NGSILD_RELATIONSHIP_OBJECT] != null ->
            Pair(NGSILD_RELATIONSHIP_OBJECT, this[NGSILD_RELATIONSHIP_OBJECT]!!)
        this[NGSILD_JSONPROPERTY_JSON] != null ->
            Pair(NGSILD_JSONPROPERTY_JSON, this[NGSILD_JSONPROPERTY_JSON]!!)
        this[NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP] != null ->
            Pair(NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP, this[NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP]!!)
        this[NGSILD_VOCABPROPERTY_VOCAB] != null ->
            Pair(NGSILD_VOCABPROPERTY_VOCAB, this[NGSILD_VOCABPROPERTY_VOCAB]!!)
        else ->
            Pair(NGSILD_PROPERTY_VALUE, this[NGSILD_PROPERTY_VALUE]!!)
    }

fun ExpandedAttributeInstance.getPropertyValue(): Any? =
    getMemberValue(NGSILD_PROPERTY_VALUE)

fun ExpandedAttributeInstance.getMemberValueAsDateTime(memberName: ExpandedTerm): ZonedDateTime? =
    ZonedDateTime::class.safeCast(this.getMemberValue(memberName))

fun ExpandedAttributeInstance.getMemberValueAsString(memberName: ExpandedTerm): String? =
    String::class.safeCast(this.getMemberValue(memberName))

fun ExpandedAttributeInstance.getRelationshipObject(name: String): Either<BadRequestDataException, URI> =
    this.right()
        .flatMap {
            if (!it.containsKey(NGSILD_RELATIONSHIP_OBJECT))
                BadRequestDataException("Relationship $name does not have an object field").left()
            else it[NGSILD_RELATIONSHIP_OBJECT]!!.right()
        }
        .flatMap {
            if (it.isEmpty())
                BadRequestDataException("Relationship $name is empty").left()
            else it[0].right()
        }
        .flatMap {
            if (it !is Map<*, *>)
                BadRequestDataException("Relationship $name has an invalid object type: ${it.javaClass}").left()
            else it[JSONLD_ID_KW].right()
        }
        .flatMap {
            if (it !is String)
                BadRequestDataException("Relationship $name has an invalid or no object id: $it").left()
            else it.toUri().right()
        }

fun ExpandedAttributeInstance.getDatasetId(): URI? =
    (this[NGSILD_DATASET_ID_IRI]?.get(0) as? Map<String, String>)?.get(JSONLD_ID_KW)?.toUri()

fun ExpandedAttributeInstance.getRelationshipId(): URI? =
    (this[NGSILD_RELATIONSHIP_OBJECT]?.get(0) as? Map<String, String>)?.get(JSONLD_ID_KW)?.toUri()

fun ExpandedAttributeInstance.getScopes(): List<String>? =
    when (val rawScopes = this.getMemberValue(NGSILD_SCOPE_IRI)) {
        is String -> listOf(rawScopes)
        is List<*> -> rawScopes as List<String>
        else -> null
    }

fun castAttributeValue(value: Any): ExpandedAttributeInstances =
    value as List<Map<String, List<Any>>>
