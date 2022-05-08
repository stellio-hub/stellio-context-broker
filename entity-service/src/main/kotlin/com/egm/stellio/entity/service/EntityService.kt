package com.egm.stellio.entity.service

import arrow.core.Either
import arrow.core.Option
import arrow.core.left
import arrow.core.right
import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.repository.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.compactedGeoPropertyKey
import com.egm.stellio.shared.util.Sub
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
            Entity(id = ngsiLdEntity.id, types = ngsiLdEntity.types, contexts = ngsiLdEntity.contexts)
        entityRepository.save(rawEntity)
        if (existsAsPartial(ngsiLdEntity.id))
            neo4jRepository.mergePartialWithNormalEntity(ngsiLdEntity.id)

        ngsiLdEntity.relationships.forEach { ngsiLdRelationship ->
            ngsiLdRelationship.instances.forEach { ngsiLdRelationshipInstance ->
                createEntityRelationship(ngsiLdEntity.id, ngsiLdRelationship.name, ngsiLdRelationshipInstance)
            }
        }

        ngsiLdEntity.properties.forEach { ngsiLdProperty ->
            ngsiLdProperty.instances.forEach { ngsiLdPropertyInstance ->
                createEntityProperty(ngsiLdEntity.id, ngsiLdProperty.name, ngsiLdPropertyInstance)
            }
        }

        ngsiLdEntity.geoProperties.forEach { ngsiLdGeoProperty ->
            // TODO we currently don't support multi-attributes for geoproperties
            createGeoProperty(ngsiLdEntity.id, ngsiLdGeoProperty.name, ngsiLdGeoProperty.instances[0])
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
        ngsiLdRelationshipInstance: NgsiLdRelationshipInstance
    ): URI {
        val rawRelationship = Relationship(relationshipType, ngsiLdRelationshipInstance)

        neo4jRepository.createRelationshipOfSubject(
            EntitySubjectNode(entityId), rawRelationship, ngsiLdRelationshipInstance.objectId
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

    internal fun createGeoProperty(
        entityId: URI,
        propertyKey: String,
        ngsiLdGeoPropertyInstance: NgsiLdGeoPropertyInstance
    ): Int {
        val compactedGeoPropertyKey = compactedGeoPropertyKey(propertyKey)
        logger.debug("Geo property $compactedGeoPropertyKey has values ${ngsiLdGeoPropertyInstance.coordinates.value}")
        return neo4jRepository.addGeoPropertyToEntity(entityId, compactedGeoPropertyKey, ngsiLdGeoPropertyInstance)
    }

    fun exists(entityId: URI): Boolean = entityRepository.existsById(entityId)

    fun checkExistence(entityId: URI): Either<APIException, Unit> =
        if (exists(entityId))
            Unit.right()
        else ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()

    fun existsAsPartial(entityId: URI): Boolean = partialEntityRepository.existsById(entityId)

    private fun serializeEntityProperties(
        properties: List<Property>,
        includeSysAttrs: Boolean = false
    ): Map<String, Any> =
        properties
            .map { property ->
                val serializedProperty = property.serializeCoreProperties(includeSysAttrs)

                property.properties.forEach { innerProperty ->
                    val serializedSubProperty = innerProperty.serializeCoreProperties(includeSysAttrs)
                    serializedProperty[innerProperty.name] = serializedSubProperty
                }

                property.relationships.forEach { innerRelationship ->
                    val serializedSubRelationship = innerRelationship.serializeCoreProperties(includeSysAttrs)
                    serializedProperty[innerRelationship.relationshipType()] = serializedSubRelationship
                }

                Pair(property.name, serializedProperty)
            }
            .groupBy({ it.first }, { it.second })

    private fun serializeEntityRelationships(
        relationships: List<Relationship>,
        includeSysAttrs: Boolean = false
    ): Map<String, Any> =
        relationships
            .map { relationship ->
                val serializedRelationship = relationship.serializeCoreProperties(includeSysAttrs)

                relationship.properties.forEach { innerProperty ->
                    val serializedSubProperty = innerProperty.serializeCoreProperties(includeSysAttrs)
                    serializedRelationship[innerProperty.name] = serializedSubProperty
                }

                relationship.relationships.forEach { innerRelationship ->
                    val serializedSubRelationship = innerRelationship.serializeCoreProperties(includeSysAttrs)
                    serializedRelationship[innerRelationship.relationshipType()] = serializedSubRelationship
                }

                Pair(relationship.relationshipType(), serializedRelationship)
            }
            .groupBy({ it.first }, { it.second })

    fun getFullEntitiesById(entitiesIds: List<URI>, includeSysAttrs: Boolean = false): List<JsonLdEntity> =
        entitiesIds
            .map {
                entityRepository.findById(it)
            }
            .filter { it.isPresent }
            .map {
                val entity = it.get()
                JsonLdEntity(
                    entity.serializeCoreProperties(includeSysAttrs)
                        .plus(serializeEntityProperties(entity.properties, includeSysAttrs))
                        .plus(serializeEntityRelationships(entity.relationships, includeSysAttrs)),
                    entity.contexts
                )
            }.sortedBy {
                // as findAllById does not preserve order of the results, sort them back by id (search order)
                it.id
            }

    /**
     * @return a pair consisting of a map representing the entity keys and attributes and the list of contexts
     * associated to the entity
     * @param includeSysAttrs true if createdAt and modifiedAt have to be displayed in the entity
     */
    fun getFullEntityById(entityId: URI, includeSysAttrs: Boolean = false): JsonLdEntity {
        val entity = entityRepository.findById(entityId)
            .orElseThrow { ResourceNotFoundException(entityNotFoundMessage(entityId.toString())) }

        return JsonLdEntity(
            entity.serializeCoreProperties(includeSysAttrs)
                .plus(serializeEntityProperties(entity.properties, includeSysAttrs))
                .plus(serializeEntityRelationships(entity.relationships, includeSysAttrs)),
            entity.contexts
        )
    }

    fun getEntityCoreProperties(entityId: URI) = entityRepository.getEntityCoreById(entityId.toString())!!

    fun getEntityTypes(entityId: URI): List<ExpandedTerm> = getEntityCoreProperties(entityId).types

    /** @param includeSysAttrs true if createdAt and modifiedAt have to be displayed in the entity
     */
    @Transactional(readOnly = true)
    fun searchEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        contextLink: String
    ): Pair<Int, List<JsonLdEntity>> =
        searchEntities(queryParams, sub, listOf(contextLink))

    /**
     * Search entities by type and query parameters
     *
     * @param queryParams the list of raw query parameters (e.g. type, idPattern,...)
     * @param contexts the list of contexts to consider
     * @param includeSysAttrs true if createdAt and modifiedAt have to be displayed in the entity
     * @return a list of entities represented as per #getFullEntityById result
     */
    @Transactional(readOnly = true)
    fun searchEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        contexts: List<String>
    ): Pair<Int, List<JsonLdEntity>> {
        val result = searchRepository.getEntities(
            queryParams,
            sub,
            contexts
        )

        return Pair(result.first, getFullEntitiesById(result.second, queryParams.includeSysAttrs))
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
                ngsiLdRelationshipInstance
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
                ngsiLdRelationshipInstance
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
            createGeoProperty(
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
            updateGeoProperty(
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

        // update modifiedAt in entity if at least one attribute has been added
        if (updateStatuses.isNotEmpty())
            neo4jRepository.updateEntityModifiedDate(id)

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
                ngsiLdRelationshipInstance
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
            neo4jRepository.deleteEntityProperty(
                EntitySubjectNode(entityId),
                ngsiLdProperty.name,
                ngsiLdPropertyInstance.datasetId
            )
            createEntityProperty(entityId, ngsiLdProperty.name, ngsiLdPropertyInstance)
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
            updateGeoProperty(entityId, ngsiLdGeoProperty.name, ngsiLdGeoProperty.instances[0])
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

    internal fun updateGeoProperty(
        entityId: URI,
        propertyKey: String,
        ngsiLdGeoPropertyInstance: NgsiLdGeoPropertyInstance
    ) {
        val compactedGeoPropertyKey = compactedGeoPropertyKey(propertyKey)
        logger.debug("Geo property $compactedGeoPropertyKey has values ${ngsiLdGeoPropertyInstance.coordinates.value}")
        neo4jRepository.updateGeoPropertyOfEntity(entityId, compactedGeoPropertyKey, ngsiLdGeoPropertyInstance)
    }

    @Transactional
    fun deleteEntity(entityId: URI): Pair<Int, Int> = neo4jRepository.deleteEntity(entityId)

    @Transactional
    fun deleteEntityAttribute(entityId: URI, expandedAttributeName: String): Boolean {
        if (neo4jRepository.hasPropertyOfName(EntitySubjectNode(entityId), expandedAttributeName)) {
            if (neo4jRepository.deleteEntityProperty(
                    subjectNodeInfo = EntitySubjectNode(entityId),
                    propertyName = expandedAttributeName,
                    deleteAll = true
                ) >= 1
            ) {
                // update modifiedAt in entity if at least one attribute has been deleted
                neo4jRepository.updateEntityModifiedDate(entityId)
                return true
            }
        } else if (neo4jRepository.hasRelationshipOfType(
                EntitySubjectNode(entityId), expandedAttributeName.toRelationshipTypeName()
            )
        ) {
            if (neo4jRepository.deleteEntityRelationship(
                    subjectNodeInfo = EntitySubjectNode(entityId),
                    relationshipType = expandedAttributeName.toRelationshipTypeName(),
                    deleteAll = true
                ) >= 1
            ) {
                // update modifiedAt in entity if at least one attribute has been deleted
                neo4jRepository.updateEntityModifiedDate(entityId)
                return true
            }
        }
        throw ResourceNotFoundException("Attribute $expandedAttributeName not found in entity $entityId")
    }

    @Transactional
    fun deleteEntityAttributeInstance(entityId: URI, expandedAttributeName: String, datasetId: URI?): Boolean {
        if (neo4jRepository.hasPropertyInstance(EntitySubjectNode(entityId), expandedAttributeName, datasetId)) {
            if (neo4jRepository.deleteEntityProperty(
                    EntitySubjectNode(entityId),
                    expandedAttributeName,
                    datasetId
                ) >= 1
            ) {
                // update modifiedAt in entity if at least one attribute has been deleted
                neo4jRepository.updateEntityModifiedDate(entityId)
                return true
            }
        } else if (neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(entityId), expandedAttributeName.toRelationshipTypeName(), datasetId
            )
        ) {
            if (neo4jRepository.deleteEntityRelationship(
                    EntitySubjectNode(entityId),
                    expandedAttributeName.toRelationshipTypeName(),
                    datasetId
                ) >= 1
            ) {
                // update modifiedAt in entity if at least one attribute has been deleted
                neo4jRepository.updateEntityModifiedDate(entityId)
                return true
            }
        }
        if (datasetId != null)
            throw ResourceNotFoundException(
                "Instance with datasetId $datasetId of $expandedAttributeName not found in entity $entityId"
            )

        throw ResourceNotFoundException("Default instance of $expandedAttributeName not found in entity $entityId")
    }
}
