package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.*
import com.egm.datahub.context.registry.repository.EntityRepository
import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.egm.datahub.context.registry.repository.PropertyRepository
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.EGM_OBSERVED_BY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_COORDINATES_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.expandJsonLdFragment
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.expandJsonLdKey
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.expandRelationshipType
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.expandValueAsMap
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.extractShortTypeFromPayload
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getPropertyValueFromMap
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getRawPropertyValueFromList
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.isOfKind
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.parseJsonLdFragment
import com.egm.datahub.context.registry.util.extractShortTypeFromExpanded
import com.egm.datahub.context.registry.util.toNgsiLdRelationshipKey
import com.egm.datahub.context.registry.util.toRelationshipTypeName
import com.egm.datahub.context.registry.web.BadRequestDataException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.utils.JsonUtils
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime
import java.util.*
import kotlin.collections.HashMap
import kotlin.reflect.full.safeCast

@Component
class Neo4jService(
    private val neo4jRepository: Neo4jRepository,
    private val entityRepository: EntityRepository,
    private val propertyRepository: PropertyRepository,
    private val applicationEventPublisher: ApplicationEventPublisher
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun createEntity(expandedPayload: Map<String, Any>, contexts: List<String>): Entity {
        val mapper = jacksonObjectMapper()
        val rawEntity = mapper.readValue(JsonUtils.toString(expandedPayload), Entity::class.java)
        // we have to re-inject the contexts as the expanded form does not ship them (by definition)
        rawEntity.contexts = contexts
        val entity = entityRepository.save(rawEntity)

        expandedPayload.filterKeys {
            !listOf("@id", "@type", "@context").contains(it)
        }.filter { entry ->
            val valuesMap = expandValueAsMap(entry.value)
            isOfKind(valuesMap, NGSILD_RELATIONSHIP_TYPE)
        }.forEach { entry ->
            val relationshipType = entry.key
            val relationshipValues = expandValueAsMap(entry.value)
            val objectId = ((relationshipValues[NGSILD_RELATIONSHIP_HAS_OBJECT] as List<Any>)[0] as Map<String, String>)["@id"]
            val objectEntity = entityRepository.findById(objectId!!)

            if (!objectEntity.isPresent)
                throw BadRequestDataException("Target entity $objectId in relationship $relationshipType does not exist, create it first")

            val relationshipEntity = createEntityRelationship(entity, relationshipType, objectEntity.get().id)

            createOutgoingRelationship(relationshipEntity, relationshipValues)
        }

        expandedPayload.filterKeys {
            !listOf("@id", "@type", "@context").contains(it)
        }.filter { entry ->
            val valuesMap = expandValueAsMap(entry.value)
            isOfKind(valuesMap, NGSILD_PROPERTY_TYPE)
        }.forEach { entry ->
            val propertyKey = entry.key
            val propertyValues = expandValueAsMap(entry.value)

            val propertyEntity = createEntityProperty(entity, propertyKey, propertyValues)

            createOutgoingRelationship(propertyEntity, propertyValues)
        }

        expandedPayload.filterKeys {
            !listOf("@id", "@type", "@context").contains(it)
        }.filter { entry ->
            val valuesMap = expandValueAsMap(entry.value)
            isOfKind(valuesMap, NGSILD_GEOPROPERTY_TYPE)
        }.forEach { entry ->
            val propertyKey = entry.key
            val propertyValues = expandValueAsMap(entry.value)

            createLocationProperty(entity, propertyKey, propertyValues)
        }

        val entityType = extractShortTypeFromPayload(expandedPayload)
        val entityEvent = EntityEvent(entityType, entity.id, EventType.POST, JsonUtils.toString(expandedPayload))
        applicationEventPublisher.publishEvent(entityEvent)

        return entity
    }

    private fun createEntityProperty(entity: Entity, propertyKey: String, propertyValues: Map<String, List<Any>>): Property {
        val propertyValue = getPropertyValueFromMap(propertyValues, NGSILD_PROPERTY_VALUE)!!
        logger.debug("Creating property $propertyKey with value $propertyValue")

        val unitCode = String::class.safeCast(getPropertyValueFromMap(propertyValues, NGSILD_UNIT_CODE_PROPERTY))
        val observedAt = OffsetDateTime::class.safeCast(getPropertyValueFromMap(propertyValues, NGSILD_OBSERVED_AT_PROPERTY))
        val property = Property(
            name = propertyKey, value = propertyValue,
            unitCode = unitCode, observedAt = observedAt
        )
        val propertyEntity = propertyRepository.save(property)
        entity.properties.add(propertyEntity)
        entityRepository.save(entity)

        return propertyEntity
    }

    private fun createEntityRelationship(entity: Entity, relationshipType: String, targetEntityId: String): Entity {
        // TODO : finish integration with relationship properties (https://redmine.eglobalmark.com/issues/847)
        val rawRelationshipEntity = Entity(type = listOf(relationshipType), id = UUID.randomUUID().toString(), contexts = emptyList())
        val relationshipEntity = entityRepository.save(rawRelationshipEntity)
        entity.relationships.add(relationshipEntity)
        entityRepository.save(entity)

        neo4jRepository.createRelationshipFromEntity(relationshipEntity.id, relationshipType.toRelationshipTypeName(), targetEntityId)

        return relationshipEntity
    }

    internal fun createLocationProperty(entity: Entity, propertyKey: String, propertyValues: Map<String, List<Any>>): Int {

        logger.debug("Geo property $propertyKey has values $propertyValues")
        val geoPropertyValue = expandValueAsMap(propertyValues[NGSILD_GEOPROPERTY_VALUE]!!)
        val geoPropertyType = geoPropertyValue["@type"]!![0] as String
        // TODO : point is not part of the NGSI-LD core context
        return if (geoPropertyType == "https://uri.etsi.org/ngsi-ld/default-context/point") {
            val geoPropertyCoordinates = geoPropertyValue[NGSILD_COORDINATES_PROPERTY]!!
            val longitude = (geoPropertyCoordinates[0] as Map<String, Double>)["@value"]
            val latitude = (geoPropertyCoordinates[1] as Map<String, Double>)["@value"]
            logger.debug("Point has coordinates $latitude, $longitude")

            neo4jRepository.addLocationPropertyToEntity(entity.id, Pair(longitude!!, latitude!!))
        } else {
            logger.warn("Unsupported geometry type : $geoPropertyType")
            0
        }
    }

    fun exists(entityId: String): Boolean = entityRepository.existsById(entityId)

    /**
     * @return a pair consisting of a map representing the entity keys and attributes and the list of contexts
     * associated to the entity
     */
    fun getFullEntityById(entityId: String): Pair<Map<String, Any>, List<String>> {
        val entity = entityRepository.getEntityCoreById(entityId)[0]["entity"] as Entity
        val resultEntity = entity.serializeCoreProperties()

        // TODO test with a property having more than one relationship (https://redmine.eglobalmark.com/issues/848)
        entityRepository.getEntitySpecificProperties(entityId)
            .forEach {
                val property = it["property"]!! as Property
                val propertyKey = property.name
                val propertyValues = mutableMapOf(
                    "@type" to NGSILD_PROPERTY_TYPE,
                    NGSILD_PROPERTY_VALUE to property.value
                )

                if (it["relOfProp"] != null) {
                    val targetEntity = it["relOfProp"] as Entity
                    val relationshipKey = (it["relType"] as String).toNgsiLdRelationshipKey()
                    logger.debug("Adding relOfProp to ${targetEntity.id} with type $relationshipKey")

                    val relationshipValue = mapOf(
                        "@type" to NGSILD_RELATIONSHIP_TYPE,
                        NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf("@id" to targetEntity.id)
                    )
                    val relationship = mapOf(relationshipKey to relationshipValue)
                    val expandedRelationshipKey = expandRelationshipType(relationship, entity.contexts)
                    propertyValues[expandedRelationshipKey] = relationshipValue
                }
                resultEntity[propertyKey] = propertyValues
            }

        entityRepository.getEntityRelationships(entityId)
            .groupBy {
                (it["relEntity"] as Entity).id
            }.values
            .forEach {
                val primaryRelType = (it[0]["relEntity"] as Entity).type[0]
                val primaryRelation =
                    it.find { relEntry -> relEntry["relType"] == primaryRelType.toRelationshipTypeName() }!!
                val relationshipTargetId = (primaryRelation["relTarget"] as Entity).id
                val relationship = mapOf(
                    "@type" to NGSILD_RELATIONSHIP_TYPE,
                    NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf("@id" to relationshipTargetId)
                )

                val relationshipValues = mutableMapOf<String, Any>()
                relationshipValues.putAll(relationship)
                val expandedRelationshipType =
                    expandRelationshipType(
                        mapOf(primaryRelType.extractShortTypeFromExpanded().toNgsiLdRelationshipKey() to relationship),
                        entity.contexts
                    )

                it.filter { relEntry -> relEntry["relType"] != primaryRelType.toRelationshipTypeName() }?.forEach {
                    val innerRelType = (it["relType"] as String).toNgsiLdRelationshipKey()
                    val innerTargetEntityId = (it["relTarget"] as Entity).id

                    val innerRelationship = mapOf(
                        "@type" to NGSILD_RELATIONSHIP_TYPE,
                        NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf("@id" to innerTargetEntityId)
                    )

                    val innerRelationshipValues = mutableMapOf<String, Any>()
                    innerRelationshipValues.putAll(innerRelationship)
                    val expandedInnerRelationshipType =
                        expandRelationshipType(mapOf(innerRelType to relationship), entity.contexts)

                    relationshipValues[expandedInnerRelationshipType] = innerRelationship
                }

                resultEntity[expandedRelationshipType] = relationshipValues
            }

        return Pair(resultEntity, entity.contexts)
    }

    fun searchEntities(type: String, query: List<String>, contextLink: String): List<Pair<Map<String, Any>, List<String>>> =
        searchEntities(type, query, listOf(contextLink))

    /**
     * Search entities by type and query parameters
     *
     * @param type the short-hand type (e.g "Measure")
     * @param query the list of raw query parameters (e.g "name==test")
     * @param contexts the list of contexts to consider
     *
     * @return a list of entities reprensented as per #getFullEntityById result
     */
    fun searchEntities(type: String, query: List<String>, contexts: List<String>): List<Pair<Map<String, Any>, List<String>>> {
        val expandedType = expandJsonLdKey(type, contexts)!!
        val queryCriteria = query
            .map {
                val splitted = it.split("==")
                val expandedParam =
                    if (splitted[1].startsWith("urn:"))
                        splitted[0].toRelationshipTypeName()
                    else
                        expandJsonLdKey(splitted[0], contexts)!!
                Pair(expandedParam, splitted[1])
            }
            .partition {
                it.second.startsWith("urn:")
            }

        return neo4jRepository.getEntitiesByTypeAndQuery(expandedType, queryCriteria)
            .map { getFullEntityById(it) }
    }

    fun updateEntityAttribute(id: String, attribute: String, payload: String, contextLink: String): Int {
        val expandedAttributeName = expandJsonLdKey(attribute, contextLink)!!
        val attributeValue = parseJsonLdFragment(payload)["value"]!!
        return neo4jRepository.updateEntityAttribute(id, expandedAttributeName, attributeValue)
    }

    fun updateEntityAttributes(id: String, payload: String, contextLink: String): List<Int> {
        val expandedPayload = expandJsonLdFragment(payload, contextLink)
        logger.debug("Expanded entity fragment to $expandedPayload (in $contextLink context)")
        return expandedPayload.map {
            // TODO check existence before eventually replacing (https://redmine.eglobalmark.com/issues/849)
            val attributeValue = expandValueAsMap(it.value)
            val attributeType = attributeValue["@type"]!![0]
            logger.debug("Fragment is of type $attributeType")
            if (attributeType == NGSILD_RELATIONSHIP_TYPE) {
                val relationshipTypeName = it.key.extractShortTypeFromExpanded()
                neo4jRepository.deleteRelationshipFromEntity(id, relationshipTypeName.toRelationshipTypeName())
                createEntityRelationship(
                    entityRepository.findById(id).get(), it.key,
                    (getRawPropertyValueFromList(it.value, NGSILD_RELATIONSHIP_HAS_OBJECT) as Map<String, Any>)["@id"] as String
                )
            } else if (attributeType == NGSILD_PROPERTY_TYPE) {
                neo4jRepository.deletePropertyFromEntity(id, it.key)
                createEntityProperty(entityRepository.findById(id).get(), it.key, attributeValue)
            }
            1
        }.toList()
    }

    fun parseEntityProperties(rawProperties: Map<String, List<Any>>): List<Property> {
        // Filter core NGSI-LD properties (observedAt, createdAt, datasetId, ...)
        // Filter @type, hasObject / hasValue
        // Filter relationships (they are created elsewhere)
        return rawProperties.filter {
            !listOf("@id", "@type", "@context").contains(it.key)
        }.filter {
            val entryValue = it.value[0]
            if (entryValue is HashMap<*, *>) {
                val entryValueAsMap = entryValue as HashMap<String, Any>
                entryValueAsMap.containsKey("@type") &&
                    entryValueAsMap["@type"] is List<*> &&
                    (entryValueAsMap as List<String>)[0] == NGSILD_PROPERTY_TYPE
            } else {
                false
            }
        }.map {
            val entryValueAsMap = it.value[0] as HashMap<String, Any>
            val propertyValue = (entryValueAsMap[NGSILD_PROPERTY_VALUE] as List<Any>)[0] as HashMap<String, Any>
            val extractedPropertyValue = propertyValue["@value"]
            logger.debug("Adding a property with value $extractedPropertyValue")

            Property(name = it.key, value = propertyValue["@value"]!!)
        }
    }

    private fun createOutgoingRelationship(subject: RelationshipTarget, values: Map<String, List<Any>>) {
        values.forEach { propEntry ->
            val propEntryValue = propEntry.value[0]
            if (propEntryValue is Map<*, *>) {
                val propEntryValueMap = propEntryValue as Map<String, Any>
                if (propEntryValueMap.containsKey("@type") &&
                    propEntryValueMap["@type"] is List<*> &&
                    (propEntryValueMap["@type"] as List<String>)[0] == NGSILD_RELATIONSHIP_TYPE) {
                    val objectId = ((propEntryValueMap[NGSILD_RELATIONSHIP_HAS_OBJECT] as List<Any>)[0] as Map<String, String>)["@id"]
                    val objectEntity = entityRepository.findById(objectId!!)
                    if (!objectEntity.isPresent)
                        throw BadRequestDataException("Target entity $objectId in property $subject does not exist, create it first")

                    // TODO there should be a way to have same code for both cases
                    if (subject is Property) {
                        neo4jRepository.createRelationshipFromProperty(subject.id,
                            propEntry.key.toRelationshipTypeName(), objectEntity.get().id)
                    } else if (subject is Entity) {
                        neo4jRepository.createRelationshipFromEntity(subject.id,
                            propEntry.key.toRelationshipTypeName(), objectEntity.get().id)
                    }
                }
            }
        }
    }

    fun deleteEntity(entityId: String): Pair<Int, Int> {
        return neo4jRepository.deleteEntity(entityId)
    }

    fun updateEntityLastMeasure(observation: Observation) {
        val observingEntity = entityRepository.findById(observation.observedBy.target)
        if (observingEntity.isEmpty) {
            logger.warn("Unable to find observing entity ${observation.observedBy.target} for observation ${observation.id}")
            return
        }

        // Find the previous observation of the same unit for the given sensor, then create or update it
        val observedByRelationshipType = EGM_OBSERVED_BY.extractShortTypeFromExpanded().toRelationshipTypeName()
        val observedProperty = neo4jRepository.getObservedProperty(observingEntity.get().id, observedByRelationshipType)

        if (observedProperty == null) {
            logger.warn("Found no property observed by ${observation.observedBy.target}, ignoring it")
        } else {
            // TODO add location into Property model (https://redmine.eglobalmark.com/issues/845)
            observedProperty.value = observation.value
            observedProperty.observedAt = observation.observedAt
            propertyRepository.save(observedProperty)
        }
    }
}
