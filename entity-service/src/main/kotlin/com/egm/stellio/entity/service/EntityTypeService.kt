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

        return EntityTypeInfo(
            id = expandedType.toUri(),
            typeName = compactTerm(expandedType, contexts),
            entityCount = attributesInformation["entityCount"] as Int,
            attributeDetails = createAttributeDetails(attributesInformation, contexts)
        )
    }

    private fun createAttributeDetails(
        attributesInformation: Map<String, Any>,
        contexts: List<String>
    ): List<AttributeInfo> {
        val propertiesAttributeInfo = createListOfAttributeInfo(
            attributesInformation["properties"] as Set<String>,
            AttributeType.Property,
            contexts
        )
        val relationshipsAttributeInfo = createListOfAttributeInfo(
            attributesInformation["relationships"] as Set<String>,
            AttributeType.Relationship,
            contexts
        )
        val geoPropertiesAttributeInfo = createListOfAttributeInfo(
            attributesInformation["geoProperties"] as Set<String>,
            AttributeType.GeoProperty,
            contexts
        )
        return listOf(propertiesAttributeInfo, relationshipsAttributeInfo, geoPropertiesAttributeInfo).flatten()
    }

    private fun createListOfAttributeInfo(
        attributesNames: Set<String>,
        attributesType: AttributeType,
        contexts: List<String>
    ): List<AttributeInfo> {
        return attributesNames.map {
            AttributeInfo(
                id = it.toUri(),
                attributeName = compactTerm(it, contexts),
                attributeTypes = listOf(attributesType)
            )
        }
    }
}
