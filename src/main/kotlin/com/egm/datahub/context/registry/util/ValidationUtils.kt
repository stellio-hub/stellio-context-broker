package com.egm.datahub.context.registry.util

import com.egm.datahub.context.registry.repository.EntityRepository
import com.egm.datahub.context.registry.service.Neo4jService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ValidationUtils(
    private val neo4jService: Neo4jService,
    private val entityRepository: EntityRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun getExistingEntities(entities: List<Pair<Map<String, Any>, List<String>>>): List<Pair<Map<String, Any>, List<String>>> {
        return entities.filter {
            neo4jService.exists(it.first.getOrElse("@id") { "" } as String)
        }
    }

    fun getNewEntities(entities: List<Pair<Map<String, Any>, List<String>>>): List<Pair<Map<String, Any>, List<String>>> {
        return entities.filter {
            !neo4jService.exists(it.first.getOrElse("@id") { "" } as String)
        }
    }

    fun getValidEntities(entities: List<Pair<Map<String, Any>, List<String>>>): Map<String, String> {
        val result: MutableMap<String, String> = mutableMapOf()
        val entitiesIds = entities.map {
            it.first.getOrElse("@id") { "" } as String
        }
        entities.forEach {
            val urn = it.first.getOrElse("@id") { "" } as String
            val relationshipIds = getRelationshipIds(urn, entitiesIds, entities, ArrayList()).first
            logger.debug("Relationships ids for $urn are $relationshipIds")
            if (entitiesIds.containsAll(relationshipIds)) {
                result[urn] = NgsiLdParsingUtils.extractTypeFromPayload(it.first)
            }
        }

        return result
    }

    fun getRelationshipIds(entityId: String, entitiesIds: List<String>, payload: List<Pair<Map<String, Any>, List<String>>>, exploredEntities: ArrayList<String>): Pair<List<String>, List<String>> {
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

    fun getEntityRelationships(entityId: String, payload: List<Pair<Map<String, Any>, List<String>>>): List<String> {
        val entityRelationshipsIds = ArrayList<String>()
        val entity = getEntityPayloadById(entityId, payload)
        val propertiesAndRelationshipsMap = entity.first.filterKeys {
            !listOf(NgsiLdParsingUtils.NGSILD_ENTITY_ID, NgsiLdParsingUtils.NGSILD_ENTITY_TYPE).contains(it)
        }.mapValues {
            NgsiLdParsingUtils.expandValueAsMap(it.value)
        }

        propertiesAndRelationshipsMap.filter { entry ->
            NgsiLdParsingUtils.isAttributeOfType(entry.value, NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE)
        }.forEach { entry ->
            val objectId = NgsiLdParsingUtils.getRelationshipObjectId(entry.value)
            val objectEntity = entityRepository.findById(objectId)
            if (!objectEntity.isPresent) {
                entityRelationshipsIds.add(objectId)
            }
        }

        return entityRelationshipsIds
    }

    fun getEntityRelationshipsOfProperties(entityId: String, payload: List<Pair<Map<String, Any>, List<String>>>): List<String> {
        val entityAttributesIds = ArrayList<String>()
        val entity = getEntityPayloadById(entityId, payload)
        val propertiesAndRelationshipsMap = entity.first.filterKeys {
            !listOf(NgsiLdParsingUtils.NGSILD_ENTITY_ID, NgsiLdParsingUtils.NGSILD_ENTITY_TYPE).contains(it)
        }.mapValues {
            NgsiLdParsingUtils.expandValueAsMap(it.value)
        }

        propertiesAndRelationshipsMap.filter { entry ->
            NgsiLdParsingUtils.isAttributeOfType(entry.value, NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE)
        }.forEach {
            it.value.forEach {
                val propEntryValue = it.value[0]
                if (propEntryValue is Map<*, *>) {
                    val propEntryValueMap = propEntryValue as Map<String, List<Any>>
                    if (NgsiLdParsingUtils.isAttributeOfType(propEntryValueMap, NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE)) {
                        val objectId = NgsiLdParsingUtils.getRelationshipObjectId(propEntryValueMap)
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

    fun getEntityPayloadById(entityId: String, payload: List<Pair<Map<String, Any>, List<String>>>): Pair<Map<String, Any>, List<String>> {
        return payload.filter {
            it.first.getOrElse("@id") { "" } as String == entityId
        }[0]
    }
}
