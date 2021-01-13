package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.egm.stellio.shared.util.toUri
import org.springframework.stereotype.Component

@Component
class EntityTypeService(
    private val entityService: EntityService,
    private val neo4jRepository: Neo4jRepository
) {

    fun getEntitiesByType(expandedType: String) =
        neo4jRepository.getEntities(emptyList(), expandedType, "")
            .mapNotNull { entityService.getFullEntityById(it, false) }

    fun getEntityTypeInformation(expandedType: String, entities: List<JsonLdEntity>): EntityTypeInfo {
        val ngsildEntities = entities.map { it.toNgsiLdEntity() }
        val propertiesNames = ngsildEntities.map { it.properties.map { it.name } }.flatten().toSet()
        val relationshipsNames = ngsildEntities.map { it.relationships.map { it.name } }.flatten().toSet()
        val geoPropertiesNames = ngsildEntities.map { it.geoProperties.map { it.name } }.flatten().toSet()

        val propertiesAttributeInfo = propertiesNames.map {
            AttributeInfo(
                id = it.toUri(),
                attributeName = it.extractShortTypeFromExpanded(),
                attributeTypes = listOf("Property")
            )
        }
        val relationshipsAttributeInfo = relationshipsNames.map {
            AttributeInfo(
                id = it.toUri(),
                attributeName = it.extractShortTypeFromExpanded(),
                attributeTypes = listOf("Relationship")
            )
        }
        val geoPropertiesAttributeInfo = geoPropertiesNames.map {
            AttributeInfo(
                id = it.toUri(),
                attributeName = it.extractShortTypeFromExpanded(),
                attributeTypes = listOf("GeoProperty")
            )
        }

        return EntityTypeInfo(
            id = expandedType.toUri(),
            typeName = expandedType.extractShortTypeFromExpanded(),
            entityCount = entities.size,
            attributeDetails = listOf(propertiesAttributeInfo, relationshipsAttributeInfo, geoPropertiesAttributeInfo)
                .flatten()
        )
    }
}
