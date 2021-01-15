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

        val propertiesAttributeInfo = (attributesInformation["properties"] as Set<String>).map {
            AttributeInfo(
                id = it.toUri(),
                attributeName = compactTerm(it, contexts),
                attributeTypes = listOf(AttributeType.Property)
            )
        }
        val relationshipsAttributeInfo = (attributesInformation["relationships"] as Set<String>).map {
            AttributeInfo(
                id = it.toUri(),
                attributeName = compactTerm(it, contexts),
                attributeTypes = listOf(AttributeType.Relationship)
            )
        }
        val geoPropertiesAttributeInfo = (attributesInformation["geoProperties"] as Set<String>).map {
            AttributeInfo(
                id = it.toUri(),
                attributeName = compactTerm(it, contexts),
                attributeTypes = listOf(AttributeType.GeoProperty)
            )
        }

        return EntityTypeInfo(
            id = expandedType.toUri(),
            typeName = compactTerm(expandedType, contexts),
            entityCount = attributesInformation["entityCount"] as Int,
            attributeDetails = listOf(propertiesAttributeInfo, relationshipsAttributeInfo, geoPropertiesAttributeInfo)
                .flatten()
        )
    }
}
