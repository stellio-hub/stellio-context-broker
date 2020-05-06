package com.egm.stellio.entity.util

import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.service.Neo4jService
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandValueAsMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.extractTypeFromPayload
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getRelationshipObjectId
import com.egm.stellio.shared.util.NgsiLdParsingUtils.isAttributeOfType
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ValidationUtils(
    private val neo4jService: Neo4jService,
    private val entityRepository: EntityRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun getExistingEntities(entities: List<ExpandedEntity>): List<ExpandedEntity> {
        return entities.filter {
            neo4jService.exists(it.getId())
        }
    }

    fun getNewEntities(entities: List<ExpandedEntity>): List<ExpandedEntity> {
        return entities.filter {
            !neo4jService.exists(it.getId())
        }
    }

    fun getValidEntities(entities: List<ExpandedEntity>): Map<String, String> {
        val result: MutableMap<String, String> = mutableMapOf()
        val entitiesIds = entities.map {
            it.getId()
        }
        entities.forEach {
            val urn = it.getId()
            val relationshipIds = getRelationshipIds(urn, entitiesIds, entities, ArrayList()).first
            logger.debug("Relationships ids for $urn are $relationshipIds")
            if (entitiesIds.containsAll(relationshipIds)) {
                result[urn] = extractTypeFromPayload(it.attributes)
            }
        }

        return result
    }

    fun getRelationshipIds(entityId: String, entitiesIds: List<String>, payload: List<ExpandedEntity>, exploredEntities: ArrayList<String>): Pair<List<String>, List<String>> {
        val result = ArrayList<String>()
        val relationshipIds = ArrayList<String>()

        exploredEntities.add(entityId)
        relationshipIds.addAll(getEntityRelationships(entityId, payload))
        relationshipIds.addAll(getEntityRelationshipsOfProperties(entityId, payload))
        for (relationshipId in relationshipIds) {
            if (!exploredEntities.contains(relationshipId)) {
                result.add(relationshipId)
                if (entitiesIds.contains(relationshipId)) {
                    val dependentIds = getRelationshipIds(relationshipId, entitiesIds, payload, exploredEntities)
                    result.addAll(dependentIds.first)
                    exploredEntities.addAll(dependentIds.second)
                }
            }
        }
        return Pair(result, exploredEntities)
    }

    fun getEntityRelationships(entityId: String, payload: List<ExpandedEntity>): List<String> {
        val entityRelationshipsIds = ArrayList<String>()
        val entity = getEntityPayloadById(entityId, payload)
        val propertiesAndRelationshipsMap = entity.attributes.filterKeys {
            !listOf(NGSILD_ENTITY_ID, NGSILD_ENTITY_TYPE).contains(it)
        }.mapValues {
            expandValueAsMap(it.value)
        }

        propertiesAndRelationshipsMap.filter { entry ->
            isAttributeOfType(entry.value, NGSILD_RELATIONSHIP_TYPE)
        }.forEach { entry ->
            val objectId = getRelationshipObjectId(entry.value)
            val objectEntity = entityRepository.findById(objectId)
            if (!objectEntity.isPresent) {
                entityRelationshipsIds.add(objectId)
            }
        }

        return entityRelationshipsIds
    }

    fun getEntityRelationshipsOfProperties(entityId: String, payload: List<ExpandedEntity>): List<String> {
        val entityAttributesIds = ArrayList<String>()
        val entity = getEntityPayloadById(entityId, payload)
        val propertiesAndRelationshipsMap = entity.attributes.filterKeys {
            !listOf(NGSILD_ENTITY_ID, NGSILD_ENTITY_TYPE).contains(it)
        }.mapValues {
            expandValueAsMap(it.value)
        }

        propertiesAndRelationshipsMap.filter { entry ->
            isAttributeOfType(entry.value, NGSILD_PROPERTY_TYPE)
        }.forEach {
            it.value.forEach {
                val propEntryValue = it.value[0]
                if (propEntryValue is Map<*, *>) {
                    val propEntryValueMap = propEntryValue as Map<String, List<Any>>
                    if (isAttributeOfType(propEntryValueMap, NGSILD_RELATIONSHIP_TYPE)) {
                        val objectId = getRelationshipObjectId(propEntryValueMap)
                        val objectEntity = entityRepository.findById(objectId)
                        if (!objectEntity.isPresent) {
                            entityAttributesIds.add(objectId)
                        }
                    }
                }
            }
        }

        return entityAttributesIds
    }

    fun getEntityPayloadById(entityId: String, payload: List<ExpandedEntity>): ExpandedEntity {
        return payload.filter {
            it.getId() == entityId
        }[0]
    }
}
