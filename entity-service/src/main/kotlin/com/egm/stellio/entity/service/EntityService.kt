package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.repository.*
import com.egm.stellio.entity.repository.AttributeSubjectNode
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.PartialEntityRepository
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.entityNotFoundMessage
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Component
class EntityService(
    private val neo4jRepository: Neo4jRepository,
    private val entityRepository: EntityRepository,
    private val partialEntityRepository: PartialEntityRepository,
    private val searchRepository: SearchRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun createEntity(ngsiLdEntity: NgsiLdEntity): URI {
        if (exists(ngsiLdEntity.id))
            throw AlreadyExistsException("Already Exists")

        val rawEntity =
            Entity(id = ngsiLdEntity.id, type = listOf(ngsiLdEntity.type), contexts = ngsiLdEntity.contexts)
        entityRepository.save(rawEntity)
        if (existsAsPartial(ngsiLdEntity.id))
            neo4jRepository.mergePartialWithNormalEntity(ngsiLdEntity.id)

        ngsiLdEntity.relationships.forEach { ngsiLdRelationship ->
            ngsiLdRelationship.instances.forEach { ngsiLdRelationshipInstance ->
                createEntityRelationship(
                    ngsiLdEntity.id,
                    ngsiLdRelationship.name,
                    ngsiLdRelationshipInstance,
                    ngsiLdRelationshipInstance.objectId
                )
            }
        }

        ngsiLdEntity.properties.forEach { ngsiLdProperty ->
            ngsiLdProperty.instances.forEach { ngsiLdPropertyInstance ->
                createEntityProperty(ngsiLdEntity.id, ngsiLdProperty.name, ngsiLdPropertyInstance)
            }
        }

        ngsiLdEntity.geoProperties.forEach { ngsiLdGeoProperty ->
            // TODO we currently don't support multi-attributes for geoproperties
            createLocationProperty(ngsiLdEntity.id, ngsiLdGeoProperty.name, ngsiLdGeoProperty.instances[0])
        }

        return ngsiLdEntity.id
    }

    /**
     * @return the id of the created property
     */
    internal fun createEntityProperty(
        entityId: URI,
        propertyKey: String,
        ngsiLdPropertyInstance: NgsiLdPropertyInstance
    ): URI {
        logger.debug("Creating property $propertyKey with value ${ngsiLdPropertyInstance.value}")

        val rawProperty = Property(propertyKey, ngsiLdPropertyInstance)

        neo4jRepository.createPropertyOfSubject(
            subjectNodeInfo = EntitySubjectNode(entityId),
            property = rawProperty
        )

        createAttributeProperties(rawProperty.id, ngsiLdPropertyInstance.properties)
        createAttributeRelationships(rawProperty.id, ngsiLdPropertyInstance.relationships)

        return rawProperty.id
    }

    /**
     * Create the relationship between two entities, as two relationships :
     *   a generic one from source entity to a relationship node,
     *   a typed one (with the relationship type) from the relationship node to the target entity.
     *
     * @return the id of the created relationship
     */
    private fun createEntityRelationship(
        entityId: URI,
        relationshipType: String,
        ngsiLdRelationshipInstance: NgsiLdRelationshipInstance,
        targetEntityId: URI
    ): URI {
        val rawRelationship = Relationship(relationshipType, ngsiLdRelationshipInstance)

        neo4jRepository.createRelationshipOfSubject(
            EntitySubjectNode(entityId), rawRelationship, targetEntityId
        )

        createAttributeProperties(rawRelationship.id, ngsiLdRelationshipInstance.properties)
        createAttributeRelationships(rawRelationship.id, ngsiLdRelationshipInstance.relationships)

        return rawRelationship.id
    }

    internal fun createAttributeProperties(subjectId: URI, properties: List<NgsiLdProperty>): Boolean =
        properties.map { ngsiLdProperty ->
            // attribute properties cannot be multi-attributes, directly get the first and unique entry
            val ngsiLdPropertyInstance = ngsiLdProperty.instances[0]
            logger.debug("Creating property ${ngsiLdProperty.name} with values ${ngsiLdPropertyInstance.value}")

            val rawProperty = Property(ngsiLdProperty.name, ngsiLdPropertyInstance)

            neo4jRepository.createPropertyOfSubject(
                subjectNodeInfo = AttributeSubjectNode(subjectId),
                property = rawProperty
            )
        }.all { it }

    internal fun createAttributeRelationships(subjectId: URI, relationships: List<NgsiLdRelationship>): Boolean =
        relationships.map { ngsiLdRelationship ->
            // attribute relationships cannot be multi-attributes, directly get the first and unique entry
            val ngsiLdRelationshipInstance = ngsiLdRelationship.instances[0]
            val objectId = ngsiLdRelationshipInstance.objectId
            val rawRelationship = Relationship(ngsiLdRelationship.name, ngsiLdRelationshipInstance)

            neo4jRepository.createRelationshipOfSubject(
                AttributeSubjectNode(subjectId),
                rawRelationship,
                objectId
            )
        }.all { it }

    internal fun createLocationProperty(
        entityId: URI,
        propertyKey: String,
        ngsiLdGeoPropertyInstance: NgsiLdGeoPropertyInstance
    ): Int {
        logger.debug("Geo property $propertyKey has values ${ngsiLdGeoPropertyInstance.coordinates}")
        // TODO : point is not part of the NGSI-LD core context (https://redmine.eglobalmark.com/issues/869)
        return when (ngsiLdGeoPropertyInstance.geoPropertyType) {
            GeoPropertyType.Point, GeoPropertyType.Polygon -> neo4jRepository.addLocationPropertyToEntity(
                entityId,
                ngsiLdGeoPropertyInstance
            )
            else -> {
                logger.warn("Unsupported geometry type : ${ngsiLdGeoPropertyInstance.geoPropertyType}")
                0
            }
        }
    }

    fun exists(entityId: URI): Boolean = entityRepository.existsById(entityId)

    fun existsAsPartial(entityId: URI): Boolean = partialEntityRepository.existsById(entityId)

    /**
     * @return a pair consisting of a map representing the entity keys and attributes and the list of contexts
     * associated to the entity
     * @param includeSysAttrs true if createdAt and modifiedAt have to be displayed in the entity
     */
    fun getFullEntityById(entityId: URI, includeSysAttrs: Boolean = false): JsonLdEntity? {
        val entity = entityRepository.findById(entityId)
            .orElseThrow { ResourceNotFoundException(entityNotFoundMessage(entityId.toString())) }
        val resultEntity = entity.serializeCoreProperties(includeSysAttrs)

        entity.properties
            .map { property ->
                val serializedProperty = property.serializeCoreProperties(includeSysAttrs)

                property.properties.forEach { innerProperty ->
                    val serializedSubProperty = innerProperty.serializeCoreProperties(includeSysAttrs)
                    serializedProperty[innerProperty.name] = serializedSubProperty
                }

                property.relationships.forEach { innerRelationship ->
                    val serializedSubRelationship = serializeRelationship(innerRelationship, includeSysAttrs)
                    serializedProperty[innerRelationship.relationshipType()] = serializedSubRelationship
                }

                Pair(property.name, serializedProperty)
            }
            .groupBy({ it.first }, { it.second })
            .forEach { (propertyName, propertyValues) ->
                resultEntity[propertyName] = propertyValues
            }

        entity.relationships
            .map { relationship ->
                val serializedRelationship = serializeRelationship(relationship, includeSysAttrs)

                relationship.properties.forEach { innerProperty ->
                    val serializedSubProperty = innerProperty.serializeCoreProperties(includeSysAttrs)
                    serializedRelationship[innerProperty.name] = serializedSubProperty
                }

                relationship.relationships.forEach { innerRelationship ->
                    val serializedSubRelationship = serializeRelationship(innerRelationship, includeSysAttrs)
                    serializedRelationship[innerRelationship.relationshipType()] = serializedSubRelationship
                }

                Pair(relationship.relationshipType(), serializedRelationship)
            }
            .groupBy({ it.first }, { it.second })
            .forEach { (relationshipName, relationshipValues) ->
                resultEntity[relationshipName] = relationshipValues
            }

        return JsonLdEntity(resultEntity, entity.contexts)
    }

    fun getEntityCoreProperties(entityId: URI) = entityRepository.getEntityCoreById(entityId.toString())!!

    private fun serializeRelationship(relationship: Relationship, includeSysAttrs: Boolean): MutableMap<String, Any> {
        val serializedRelationship = relationship.serializeCoreProperties(includeSysAttrs)
        // TODO not perfect as it makes one more DB request per relationship to get the target entity id
        val targetEntityId =
            neo4jRepository.getTargetObjectIdOfRelationship(
                relationship.id,
                relationship.relationshipType().toRelationshipTypeName()
            )
        serializedRelationship[NGSILD_RELATIONSHIP_HAS_OBJECT] = mapOf(JSONLD_ID to targetEntityId.toString())
        return serializedRelationship
    }

    /** @param includeSysAttrs true if createdAt and modifiedAt have to be displayed in the entity
     */
    fun searchEntities(
        queryParams: QueryParams,
        userSub: String,
        offset: Int,
        limit: Int,
        contextLink: String,
        includeSysAttrs: Boolean
    ): Pair<Int, List<JsonLdEntity>> =
        searchEntities(queryParams, userSub, offset, limit, listOf(contextLink), includeSysAttrs)

    /**
     * Search entities by type and query parameters
     *
     * @param type the short-hand type (e.g "Measure")
     * @param query the list of raw query parameters (e.g "name==test")
     * @param contexts the list of contexts to consider
     * @param includeSysAttrs true if createdAt and modifiedAt have to be displayed in the entity
     * @return a list of entities represented as per #getFullEntityById result
     */
    @Transactional
    fun searchEntities(
        queryParams: QueryParams,
        userSub: String,
        offset: Int,
        limit: Int,
        contexts: List<String>,
        includeSysAttrs: Boolean
    ): Pair<Int, List<JsonLdEntity>> {
        val result = searchRepository.getEntities(
            queryParams,
            userSub,
            offset,
            limit,
            contexts
        )
        return Pair(result.first, result.second.mapNotNull { getFullEntityById(it, includeSysAttrs) })
    }

    @Transactional
    fun appendEntityAttributes(
        entityId: URI,
        attributes: List<NgsiLdAttribute>,
        disallowOverwrite: Boolean
    ): UpdateResult {
        val updateStatuses = attributes
            .flatMap { ngsiLdAttribute ->
                logger.debug("Fragment is of type $ngsiLdAttribute (${ngsiLdAttribute.compactName})")
                when (ngsiLdAttribute) {
                    is NgsiLdRelationship -> ngsiLdAttribute.instances.map { ngsiLdRelationshipInstance ->
                        appendEntityRelationship(
                            entityId,
                            ngsiLdAttribute,
                            ngsiLdRelationshipInstance,
                            disallowOverwrite
                        )
                    }
                    is NgsiLdProperty -> ngsiLdAttribute.instances.map { ngsiLdPropertyInstance ->
                        appendEntityProperty(entityId, ngsiLdAttribute, ngsiLdPropertyInstance, disallowOverwrite)
                    }
                    is NgsiLdGeoProperty ->
                        listOf(appendEntityGeoProperty(entityId, ngsiLdAttribute, disallowOverwrite))
                }
            }

        // update modifiedAt in entity if at least one attribute has been added
        if (updateStatuses.isNotEmpty())
            neo4jRepository.updateEntityModifiedDate(entityId)

        return updateResultFromDetailedResult(updateStatuses)
    }

    fun appendEntityRelationship(
        entityId: URI,
        ngsiLdRelationship: NgsiLdRelationship,
        ngsiLdRelationshipInstance: NgsiLdRelationshipInstance,
        disallowOverwrite: Boolean
    ): UpdateAttributeResult {
        val relationshipTypeName = ngsiLdRelationship.name.extractShortTypeFromExpanded()
        return if (!neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(entityId),
                relationshipTypeName,
                ngsiLdRelationshipInstance.datasetId
            )
        ) {
            createEntityRelationship(
                entityId,
                ngsiLdRelationship.name,
                ngsiLdRelationshipInstance,
                ngsiLdRelationshipInstance.objectId
            )
            UpdateAttributeResult(
                ngsiLdRelationship.name,
                ngsiLdRelationshipInstance.datasetId,
                UpdateOperationResult.APPENDED,
                null
            )
        } else if (disallowOverwrite) {
            logger.info(
                "Relationship $relationshipTypeName already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
            UpdateAttributeResult(
                ngsiLdRelationship.name,
                ngsiLdRelationshipInstance.datasetId,
                UpdateOperationResult.IGNORED,
                "Relationship $relationshipTypeName already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
        } else {
            neo4jRepository.deleteEntityRelationship(
                EntitySubjectNode(entityId),
                relationshipTypeName,
                ngsiLdRelationshipInstance.datasetId
            )
            createEntityRelationship(
                entityId,
                ngsiLdRelationship.name,
                ngsiLdRelationshipInstance,
                ngsiLdRelationshipInstance.objectId
            )
            UpdateAttributeResult(
                ngsiLdRelationship.name,
                ngsiLdRelationshipInstance.datasetId,
                UpdateOperationResult.REPLACED,
                null
            )
        }
    }

    fun appendEntityProperty(
        entityId: URI,
        ngsiLdProperty: NgsiLdProperty,
        ngsiLdPropertyInstance: NgsiLdPropertyInstance,
        disallowOverwrite: Boolean
    ): UpdateAttributeResult {
        return if (!neo4jRepository.hasPropertyInstance(
                EntitySubjectNode(entityId),
                ngsiLdProperty.name,
                ngsiLdPropertyInstance.datasetId
            )
        ) {
            createEntityProperty(entityId, ngsiLdProperty.name, ngsiLdPropertyInstance)
            UpdateAttributeResult(
                ngsiLdProperty.name,
                ngsiLdPropertyInstance.datasetId,
                UpdateOperationResult.APPENDED,
                null
            )
        } else if (disallowOverwrite) {
            logger.info(
                "Property ${ngsiLdProperty.name} already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
            UpdateAttributeResult(
                ngsiLdProperty.name,
                ngsiLdPropertyInstance.datasetId,
                UpdateOperationResult.IGNORED,
                "Property ${ngsiLdProperty.name} already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
        } else {
            neo4jRepository.deleteEntityProperty(
                EntitySubjectNode(entityId),
                ngsiLdProperty.name,
                ngsiLdPropertyInstance.datasetId
            )
            createEntityProperty(entityId, ngsiLdProperty.name, ngsiLdPropertyInstance)
            UpdateAttributeResult(
                ngsiLdProperty.name,
                ngsiLdPropertyInstance.datasetId,
                UpdateOperationResult.REPLACED,
                null
            )
        }
    }

    fun appendEntityGeoProperty(
        entityId: URI,
        ngsiLdGeoProperty: NgsiLdGeoProperty,
        disallowOverwrite: Boolean
    ): UpdateAttributeResult {
        return if (!neo4jRepository.hasGeoPropertyOfName(
                EntitySubjectNode(entityId),
                ngsiLdGeoProperty.name.extractShortTypeFromExpanded()
            )
        ) {
            createLocationProperty(
                entityId,
                ngsiLdGeoProperty.name,
                ngsiLdGeoProperty.instances[0]
            )
            UpdateAttributeResult(
                ngsiLdGeoProperty.name,
                ngsiLdGeoProperty.instances[0].datasetId,
                UpdateOperationResult.APPENDED,
                null
            )
        } else if (disallowOverwrite) {
            logger.info(
                "GeoProperty ${ngsiLdGeoProperty.name} already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
            UpdateAttributeResult(
                ngsiLdGeoProperty.name,
                ngsiLdGeoProperty.instances[0].datasetId,
                UpdateOperationResult.IGNORED,
                "GeoProperty ${ngsiLdGeoProperty.name} already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
        } else {
            updateLocationPropertyOfEntity(
                entityId,
                ngsiLdGeoProperty.name,
                ngsiLdGeoProperty.instances[0]
            )
            UpdateAttributeResult(
                ngsiLdGeoProperty.name,
                ngsiLdGeoProperty.instances[0].datasetId,
                UpdateOperationResult.REPLACED,
                null
            )
        }
    }

    @Transactional
    fun updateEntityAttributes(id: URI, attributes: List<NgsiLdAttribute>): UpdateResult {
        val updateStatuses = attributes.flatMap { ngsiLdAttribute ->
            try {
                logger.debug("Trying to update attribute ${ngsiLdAttribute.name} of type $ngsiLdAttribute")
                when (ngsiLdAttribute) {
                    is NgsiLdRelationship -> ngsiLdAttribute.instances.map { ngsiLdRelationshipInstance ->
                        updateEntityRelationship(id, ngsiLdAttribute, ngsiLdRelationshipInstance)
                    }
                    is NgsiLdProperty -> ngsiLdAttribute.instances.map { ngsiLdPropertyInstance ->
                        updateEntityProperty(id, ngsiLdAttribute, ngsiLdPropertyInstance)
                    }
                    is NgsiLdGeoProperty -> listOf(updateEntityGeoProperty(id, ngsiLdAttribute))
                }
            } catch (e: BadRequestDataException) {
                listOf(
                    UpdateAttributeResult(ngsiLdAttribute.name, null, UpdateOperationResult.IGNORED, e.message)
                )
            }
        }
        return updateResultFromDetailedResult(updateStatuses)
    }

    fun updateEntityRelationship(
        entityId: URI,
        ngsiLdRelationship: NgsiLdRelationship,
        ngsiLdRelationshipInstance: NgsiLdRelationshipInstance
    ): UpdateAttributeResult =
        if (neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(entityId),
                ngsiLdRelationship.name.toRelationshipTypeName(),
                ngsiLdRelationshipInstance.datasetId
            )
        ) {
            deleteEntityAttributeInstance(entityId, ngsiLdRelationship.name, ngsiLdRelationshipInstance.datasetId)
            createEntityRelationship(
                entityId,
                ngsiLdRelationship.name,
                ngsiLdRelationshipInstance,
                ngsiLdRelationshipInstance.objectId
            )
            UpdateAttributeResult(
                ngsiLdRelationship.name,
                ngsiLdRelationshipInstance.datasetId,
                UpdateOperationResult.REPLACED
            )
        } else {
            val message = if (ngsiLdRelationshipInstance.datasetId != null)
                "Relationship (datasetId: ${ngsiLdRelationshipInstance.datasetId}) does not exist"
            else
                "Relationship (default instance) does not exist"
            UpdateAttributeResult(
                ngsiLdRelationship.name,
                ngsiLdRelationshipInstance.datasetId,
                UpdateOperationResult.IGNORED,
                message
            )
        }

    fun updateEntityProperty(
        entityId: URI,
        ngsiLdProperty: NgsiLdProperty,
        ngsiLdPropertyInstance: NgsiLdPropertyInstance
    ): UpdateAttributeResult =
        if (neo4jRepository.hasPropertyInstance(
                EntitySubjectNode(entityId), ngsiLdProperty.name, ngsiLdPropertyInstance.datasetId
            )
        ) {
            updateEntityAttributeInstance(entityId, ngsiLdProperty.name, ngsiLdPropertyInstance)
            UpdateAttributeResult(
                ngsiLdProperty.name,
                ngsiLdPropertyInstance.datasetId,
                UpdateOperationResult.REPLACED
            )
        } else {
            val message = if (ngsiLdPropertyInstance.datasetId != null)
                "Property (datasetId: ${ngsiLdPropertyInstance.datasetId}) does not exist"
            else
                "Property (default instance) does not exist"
            UpdateAttributeResult(
                ngsiLdProperty.name,
                ngsiLdPropertyInstance.datasetId,
                UpdateOperationResult.IGNORED,
                message
            )
        }

    fun updateEntityGeoProperty(entityId: URI, ngsiLdGeoProperty: NgsiLdGeoProperty): UpdateAttributeResult =
        if (neo4jRepository.hasGeoPropertyOfName(EntitySubjectNode(entityId), ngsiLdGeoProperty.compactName)) {
            updateLocationPropertyOfEntity(entityId, ngsiLdGeoProperty.name, ngsiLdGeoProperty.instances[0])
            UpdateAttributeResult(
                ngsiLdGeoProperty.name,
                ngsiLdGeoProperty.instances[0].datasetId,
                UpdateOperationResult.REPLACED
            )
        } else
            UpdateAttributeResult(
                ngsiLdGeoProperty.name,
                ngsiLdGeoProperty.instances[0].datasetId,
                UpdateOperationResult.IGNORED,
                "GeoProperty does not exist"
            )

    internal fun updateLocationPropertyOfEntity(
        entityId: URI,
        propertyKey: String,
        ngsiLdGeoPropertyInstance: NgsiLdGeoPropertyInstance
    ) {
        logger.debug("Geo property $propertyKey has values ${ngsiLdGeoPropertyInstance.coordinates}")
        // TODO : point is not part of the NGSI-LD core context (https://redmine.eglobalmark.com/issues/869)
        when (ngsiLdGeoPropertyInstance.geoPropertyType) {
            GeoPropertyType.Point, GeoPropertyType.Polygon -> neo4jRepository.updateLocationPropertyOfEntity(
                entityId,
                ngsiLdGeoPropertyInstance
            )
            else -> throw BadRequestDataException(
                "Unsupported geometry type : ${ngsiLdGeoPropertyInstance.geoPropertyType}"
            )
        }
    }

    @Transactional
    fun deleteEntity(entityId: URI) = neo4jRepository.deleteEntity(entityId)

    @Transactional
    fun deleteEntityAttribute(entityId: URI, expandedAttributeName: String): Boolean {
        if (neo4jRepository.hasPropertyOfName(EntitySubjectNode(entityId), expandedAttributeName))
            return neo4jRepository.deleteEntityProperty(
                subjectNodeInfo = EntitySubjectNode(entityId), propertyName = expandedAttributeName, deleteAll = true
            ) >= 1
        else if (neo4jRepository.hasRelationshipOfType(
                EntitySubjectNode(entityId), expandedAttributeName.toRelationshipTypeName()
            )
        )
            return neo4jRepository.deleteEntityRelationship(
                subjectNodeInfo = EntitySubjectNode(entityId),
                relationshipType = expandedAttributeName.toRelationshipTypeName(), deleteAll = true
            ) >= 1

        throw ResourceNotFoundException("Attribute $expandedAttributeName not found in entity $entityId")
    }

    @Transactional
    fun deleteEntityAttributeInstance(entityId: URI, expandedAttributeName: String, datasetId: URI?): Boolean {
        if (neo4jRepository.hasPropertyInstance(EntitySubjectNode(entityId), expandedAttributeName, datasetId))
            return neo4jRepository.deleteEntityProperty(
                EntitySubjectNode(entityId),
                expandedAttributeName, datasetId
            ) >= 1
        else if (neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(entityId), expandedAttributeName.toRelationshipTypeName(), datasetId
            )
        )
            return neo4jRepository.deleteEntityRelationship(
                EntitySubjectNode(entityId),
                expandedAttributeName.toRelationshipTypeName(),
                datasetId
            ) >= 1

        if (datasetId != null)
            throw ResourceNotFoundException(
                "Instance with datasetId $datasetId of $expandedAttributeName not found in entity $entityId"
            )

        throw ResourceNotFoundException("Default instance of $expandedAttributeName not found in entity $entityId")
    }

    // TODO add support for update relationship instance
    @Transactional
    fun updateEntityAttributeInstance(
        entityId: URI,
        expandedAttributeName: String,
        ngsiLdPropertyInstance: NgsiLdPropertyInstance
    ) =
        neo4jRepository.updateEntityPropertyInstance(
            EntitySubjectNode(entityId),
            expandedAttributeName,
            ngsiLdPropertyInstance
        ) >= 1
}
