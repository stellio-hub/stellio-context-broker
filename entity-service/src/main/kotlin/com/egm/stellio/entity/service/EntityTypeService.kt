package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.toUri
import org.springframework.stereotype.Component

@Component
class EntityTypeService(
    private val neo4jRepository: Neo4jRepository
) {

    fun getEntityTypeInfo(expandedType: String, contexts: List<String>): EntityTypeInfo? {
        val attributesInformation = neo4jRepository.getEntityTypeAttributesInformation(expandedType)
        if (attributesInformation.isEmpty()) return null

        val propertiesAttributeInfo =
            createAttributeDetails(attributesInformation, "properties", AttributeType.Property, contexts)
        val relationshipsAttributeInfo =
            createAttributeDetails(attributesInformation, "relationships", AttributeType.Relationship, contexts)
        val geoPropertiesAttributeInfo =
            createAttributeDetails(attributesInformation, "geoProperties", AttributeType.GeoProperty, contexts)

        return EntityTypeInfo(
            id = expandedType.toUri(),
            typeName = compactTerm(expandedType, contexts),
            entityCount = attributesInformation["entityCount"] as Int,
            attributeDetails = propertiesAttributeInfo.plus(relationshipsAttributeInfo).plus(geoPropertiesAttributeInfo)
        )
    }

    private fun createAttributeDetails(
        attributesInformation: Map<String, Any>,
        key: String,
        attributesType: AttributeType,
        contexts: List<String>
    ): List<AttributeInfo> =
        (attributesInformation[key] as Set<String>).map {
            AttributeInfo(
                id = it.toUri(),
                attributeName = compactTerm(it, contexts),
                attributeTypes = listOf(attributesType)
            )
        }
}
