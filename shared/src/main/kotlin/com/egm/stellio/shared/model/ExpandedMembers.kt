package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
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
    entityId: String,
    entityTypes: List<ExpandedTerm>
): Map<String, Any> =
    this.plus(listOf(JSONLD_ID to entityId, JSONLD_TYPE to entityTypes))

fun ExpandedAttributes.getAttributeFromExpandedAttributes(
    expandedAttributeName: ExpandedTerm,
    datasetId: URI?
): ExpandedAttributeInstance? =
    this[expandedAttributeName]?.let { expandedAttributeInstances ->
        expandedAttributeInstances.find { expandedAttributeInstance ->
            if (datasetId == null)
                !expandedAttributeInstance.containsKey(NGSILD_DATASET_ID_PROPERTY)
            else
                expandedAttributeInstance.getMemberValue(NGSILD_DATASET_ID_PROPERTY) == datasetId.toString()
        }
    }

fun ExpandedAttribute.toExpandedAttributes(): ExpandedAttributes =
    mapOf(this.first to this.second)

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
    return listOf(this[0].plus(subAttributeName to JsonLdUtils.buildNonReifiedPropertyValue(subAttributeValue)))
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
    modifiedAt: ZonedDateTime?
): Map<String, Any> =
    if (withSysAttrs)
        this.plus(NGSILD_CREATED_AT_PROPERTY to buildNonReifiedTemporalValue(createdAt))
            .let {
                if (modifiedAt != null)
                    it.plus(NGSILD_MODIFIED_AT_PROPERTY to buildNonReifiedTemporalValue(modifiedAt))
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
        val finalValueType = firstListEntry[JSONLD_TYPE]
        when {
            finalValueType != null -> {
                val finalValue = String::class.safeCast(firstListEntry[JSONLD_VALUE])
                when (finalValueType) {
                    NGSILD_DATE_TIME_TYPE -> ZonedDateTime.parse(finalValue)
                    NGSILD_DATE_TYPE -> LocalDate.parse(finalValue)
                    NGSILD_TIME_TYPE -> LocalTime.parse(finalValue)
                    else -> firstListEntry[JSONLD_VALUE]
                }
            }

            firstListEntry[JSONLD_VALUE] != null ->
                firstListEntry[JSONLD_VALUE]

            firstListEntry[JSONLD_ID] != null -> {
                // Used to get the value of datasetId property,
                // since it is mapped to "@id" key rather than "@value"
                firstListEntry[JSONLD_ID]
            }

            else -> {
                // it is a map / JSON object, keep it as is
                // {https://uri.etsi.org/ngsi-ld/default-context/key=[{@value=value}], ...}
                firstListEntry
            }
        }
    } else {
        intermediateList.map {
            it[JSONLD_VALUE]
        }
    }
}

fun ExpandedAttributeInstance.getMemberValueAsDateTime(memberName: ExpandedTerm): ZonedDateTime? =
    ZonedDateTime::class.safeCast(this.getMemberValue(memberName))

fun ExpandedAttributeInstance.getMemberValueAsString(memberName: ExpandedTerm): String? =
    String::class.safeCast(this.getMemberValue(memberName))

fun ExpandedAttributeInstance.extractRelationshipObject(name: String): Either<BadRequestDataException, URI> =
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
            else it[JSONLD_ID].right()
        }
        .flatMap {
            if (it !is String)
                BadRequestDataException("Relationship $name has an invalid or no object id: $it").left()
            else it.toUri().right()
        }

fun ExpandedAttributeInstance.getDatasetId(): URI? =
    (this[NGSILD_DATASET_ID_PROPERTY]?.get(0) as? Map<String, String>)?.get(JSONLD_ID)?.toUri()

fun ExpandedAttributeInstance.getScopes(): List<String>? =
    when (val rawScopes = this.getMemberValue(NGSILD_SCOPE_PROPERTY)) {
        is String -> listOf(rawScopes)
        is List<*> -> rawScopes as List<String>
        else -> null
    }

fun ExpandedAttributeInstance.getPropertyValue(): Any {
    val hasValueEntry = this[NGSILD_PROPERTY_VALUE]!!

    return if (hasValueEntry.size == 1 && (hasValueEntry[0] as Map<String, Any>).containsKey(JSONLD_VALUE)) {
        val rawValue = (hasValueEntry[0] as Map<String, Any>)[JSONLD_VALUE]!!
        if (rawValue is String) {
            when {
                rawValue.isURI() -> rawValue.toUri()
                rawValue.isTime() -> LocalTime.parse(rawValue)
                rawValue.isDate() -> LocalDate.parse(rawValue)
                rawValue.isDateTime() -> ZonedDateTime.parse(rawValue)
                else -> rawValue
            }
        } else rawValue
    } else if (hasValueEntry.size == 1)
        hasValueEntry[0] as Map<String, Any>
    else hasValueEntry.map { (it as Map<String, Any>)[JSONLD_VALUE]!! }
}

fun castAttributeValue(value: Any): ExpandedAttributeInstances =
    value as List<Map<String, List<Any>>>
