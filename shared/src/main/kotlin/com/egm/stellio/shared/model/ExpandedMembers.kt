package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import java.time.ZonedDateTime

// basic alias to help identify, mainly in method calls, if the expected value is a compact or expanded one
typealias ExpandedTerm = String
typealias ExpandedAttributes = Map<ExpandedTerm, ExpandedAttributeInstances>
typealias ExpandedAttribute = Pair<ExpandedTerm, ExpandedAttributeInstances>
typealias ExpandedAttributeInstances = List<ExpandedAttributeInstance>
typealias ExpandedAttributeInstance = Map<String, List<Any>>
typealias ExpandedNonReifiedPropertyValue = List<Map<String, Any>>

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

fun ExpandedAttributes.addCoreMembers(
    entityId: String,
    entityTypes: List<ExpandedTerm>
): Map<String, Any> =
    this.plus(listOf(JSONLD_ID to entityId, JSONLD_TYPE to entityTypes))

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
