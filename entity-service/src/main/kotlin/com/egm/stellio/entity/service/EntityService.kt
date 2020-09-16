package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.entity.repository.AttributeSubjectNode
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.PropertyRepository
import com.egm.stellio.entity.util.EntitiesGraphBuilder
import com.egm.stellio.entity.util.extractComparaisonParametersFromQuery
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdGeoProperty
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.NgsiLdProperty
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.model.NgsiLdRelationship
import com.egm.stellio.shared.model.NgsiLdRelationshipInstance
import com.egm.stellio.shared.model.Observation
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.JsonLdUtils.EGM_VENDOR_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compactAndStringifyFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonLdUtils.expandRelationshipType
import com.egm.stellio.shared.util.JsonLdUtils.parseJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import org.neo4j.ogm.types.spatial.GeographicPoint2d
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Component
class EntityService(
    private val neo4jRepository: Neo4jRepository,
    private val entityRepository: EntityRepository,
    private val entitiesGraphBuilder: EntitiesGraphBuilder,
    private val propertyRepository: PropertyRepository,
    private val applicationEventPublisher: ApplicationEventPublisher
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun createEntity(ngsiLdEntity: NgsiLdEntity): Entity {
        if (exists(ngsiLdEntity.id)) {
            throw AlreadyExistsException("Already Exists")
        }

        val (_, invalidRelationsErrors) = entitiesGraphBuilder.build(listOf(ngsiLdEntity))
        if (invalidRelationsErrors.isNotEmpty()) {
            // we have just one entity in validation, so can't have more than one relations error
            // in its current version, it will only contain one error but it will be fixed with validation v2
            val inErrorRelationships = invalidRelationsErrors[0].error.joinToString(",")
            throw BadRequestDataException("Entity ${ngsiLdEntity.id} targets unknown entities: $inErrorRelationships")
        }

        val rawEntity =
            Entity(id = ngsiLdEntity.id, type = listOf(ngsiLdEntity.type), contexts = ngsiLdEntity.contexts)
        val entity = entityRepository.save(rawEntity)

        ngsiLdEntity.relationships.forEach { ngsiLdRelationship ->
            ngsiLdRelationship.instances.forEach { ngsiLdRelationshipInstance ->
                createEntityRelationship(
                    entity.id,
                    ngsiLdRelationship.name,
                    ngsiLdRelationshipInstance,
                    ngsiLdRelationshipInstance.objectId
                )
            }
        }

        ngsiLdEntity.properties.forEach { ngsiLdProperty ->
            ngsiLdProperty.instances.forEach { ngsiLdPropertyInstance ->
                createEntityProperty(entity.id, ngsiLdProperty.name, ngsiLdPropertyInstance)
            }
        }

        ngsiLdEntity.geoProperties.forEach { ngsiLdGeoProperty ->
            // TODO we currently don't support multi-attributes for geoproperties
            createLocationProperty(entity.id, ngsiLdGeoProperty.name, ngsiLdGeoProperty.instances[0])
        }

        publishCreationEvent(ngsiLdEntity)

        return entity
    }

    fun publishCreationEvent(ngsiLdEntity: NgsiLdEntity) {
        getSerializedEntityById(ngsiLdEntity.id)?.also {
            val entityEvent = EntityEvent(
                operationType = EventType.CREATE,
                entityId = ngsiLdEntity.id,
                entityType = ngsiLdEntity.type.extractShortTypeFromExpanded(),
                payload = it
            )
            applicationEventPublisher.publishEvent(entityEvent)
        }
    }

    fun publishDeletionEvent(entityId: URI) {
        getSerializedEntityById(entityId)?.also {
            val entityEvent = EntityEvent(
                operationType = EventType.DELETE,
                entityId = entityId
            )
            applicationEventPublisher.publishEvent(entityEvent)
        }
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
        // TODO : finish integration with relationship properties (https://redmine.eglobalmark.com/issues/847)
        val rawRelationship = Relationship(relationshipType, ngsiLdRelationshipInstance)

        neo4jRepository.createRelationshipOfSubject(
            EntitySubjectNode(entityId), rawRelationship, targetEntityId
        )

        createAttributeProperties(rawRelationship.id, ngsiLdRelationshipInstance.properties)
        createAttributeRelationships(rawRelationship.id, ngsiLdRelationshipInstance.relationships)

        return rawRelationship.id
    }

    private fun createAttributeProperties(subjectId: URI, properties: List<NgsiLdProperty>) {
        properties.forEach { ngsiLdProperty ->
            // attribute properties cannot be multi-attributes, directly get the first and unique entry
            val ngsiLdPropertyInstance = ngsiLdProperty.instances[0]
            logger.debug("Creating property ${ngsiLdProperty.name} with values ${ngsiLdPropertyInstance.value}")

            val rawProperty = Property(ngsiLdProperty.name, ngsiLdPropertyInstance)

            neo4jRepository.createPropertyOfSubject(
                subjectNodeInfo = AttributeSubjectNode(subjectId),
                property = rawProperty
            )
        }
    }

    private fun createAttributeRelationships(subjectId: URI, relationships: List<NgsiLdRelationship>) {
        relationships.forEach { ngsiLdRelationship ->
            // attribute relationships cannot be multi-attributes, directly get the first and unique entry
            val ngsiLdRelationshipInstance = ngsiLdRelationship.instances[0]
            val objectId = ngsiLdRelationshipInstance.objectId
            if (exists(objectId)) {
                val rawRelationship = Relationship(ngsiLdRelationship.name, ngsiLdRelationshipInstance)

                neo4jRepository.createRelationshipOfSubject(
                    AttributeSubjectNode(subjectId),
                    rawRelationship,
                    objectId
                )
            } else {
                // TODO early validation
                throw BadRequestDataException(
                    "Target entity $objectId in property $subjectId does not exist, create it first"
                )
            }
        }
    }

    internal fun createLocationProperty(
        entityId: URI,
        propertyKey: String,
        ngsiLdGeoPropertyInstance: NgsiLdGeoPropertyInstance
    ): Int {
        logger.debug("Geo property $propertyKey has values ${ngsiLdGeoPropertyInstance.coordinates}")
        // TODO : point is not part of the NGSI-LD core context (https://redmine.eglobalmark.com/issues/869)
        return if (ngsiLdGeoPropertyInstance.geoPropertyType == "Point") {
            neo4jRepository.addLocationPropertyToEntity(
                entityId,
                Pair(
                    ngsiLdGeoPropertyInstance.coordinates[0] as Double,
                    ngsiLdGeoPropertyInstance.coordinates[1] as Double
                )
            )
        } else {
            logger.warn("Unsupported geometry type : ${ngsiLdGeoPropertyInstance.geoPropertyType}")
            0
        }
    }

    fun exists(entityId: URI): Boolean = entityRepository.exists(entityId.toString()) ?: false

    /**
     * @return a pair consisting of a map representing the entity keys and attributes and the list of contexts
     * associated to the entity
     * @param includeSysAttrs true if createdAt and modifiedAt have to be displayed in the entity
     */
    fun getFullEntityById(entityId: URI, includeSysAttrs: Boolean = false): JsonLdEntity? {
        val entity = entityRepository.getEntityCoreById(entityId.toString()) ?: return null
        val resultEntity = entity.serializeCoreProperties(includeSysAttrs)

        // TODO test with a property having more than one relationship (https://redmine.eglobalmark.com/issues/848)
        entityRepository.getEntitySpecificProperties(entityId.toString())
            .groupBy {
                (it["property"] as Property).id
            }
            .values
            .map { buildPropertyFragment(it, entity.contexts, includeSysAttrs) }
            .groupBy { it.first }
            .mapValues { propertyInstances ->
                propertyInstances.value.map { instanceFragment ->
                    instanceFragment.second
                }
            }
            .forEach { property ->
                resultEntity[property.key] = property.value
            }

        entityRepository.getEntityRelationships(entityId.toString())
            .groupBy {
                (it["rel"] as Relationship).id
            }.values
            .map {
                buildRelationshipFragment(it, entity.contexts, includeSysAttrs)
            }
            .groupBy { it.first }
            .mapValues { relationshipInstances ->
                relationshipInstances.value.map { instanceFragment ->
                    instanceFragment.second
                }
            }
            .forEach { relationship ->
                resultEntity[relationship.key] = relationship.value
            }

        return JsonLdEntity(resultEntity, entity.contexts)
    }

    private fun buildPropertyFragment(
        rawProperty: List<Map<String, Any>>,
        contexts: List<String>,
        includeSysAttrs: Boolean
    ): Pair<String, Map<String, Any>> {
        val property = rawProperty[0]["property"]!! as Property
        val propertyKey = property.name
        val propertyValues = property.serializeCoreProperties(includeSysAttrs)

        rawProperty.filter { relEntry -> relEntry["propValue"] != null }
            .forEach {
                val propertyOfProperty = it["propValue"] as Property
                propertyValues[propertyOfProperty.name] = propertyOfProperty.serializeCoreProperties(includeSysAttrs)
            }

        rawProperty.filter { relEntry -> relEntry["relOfProp"] != null }
            .forEach {
                val relationship = it["relOfProp"] as Relationship
                val targetEntity = it["relOfPropObject"] as Entity
                val relationshipKey = (it["relType"] as String)
                logger.debug("Adding relOfProp to ${targetEntity.id} with type $relationshipKey")

                val relationshipValue = mapOf(
                    JSONLD_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                    NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(JSONLD_ID to targetEntity.id)
                )
                val relationshipValues = relationship.serializeCoreProperties(includeSysAttrs)
                relationshipValues.putAll(relationshipValue)
                val expandedRelationshipKey =
                    expandRelationshipType(mapOf(relationshipKey to relationshipValue), contexts)
                propertyValues[expandedRelationshipKey] = relationshipValues
            }

        return Pair(propertyKey, propertyValues)
    }

    private fun buildRelationshipFragment(
        rawRelationship: List<Map<String, Any>>,
        contexts: List<String>,
        includeSysAttrs: Boolean
    ): Pair<String, Map<String, Any>> {
        val relationship = rawRelationship[0]["rel"] as Relationship
        val primaryRelType = (rawRelationship[0]["rel"] as Relationship).type[0]
        val primaryRelation =
            rawRelationship.find { relEntry -> relEntry["relType"] == primaryRelType.toRelationshipTypeName() }!!
        val relationshipTargetId = (primaryRelation["relObject"] as Entity).id
        val relationshipValue = mapOf(
            JSONLD_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
            NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(JSONLD_ID to relationshipTargetId)
        )

        val relationshipValues = relationship.serializeCoreProperties(includeSysAttrs)
        relationshipValues.putAll(relationshipValue)

        rawRelationship.filter { relEntry -> relEntry["relOfRel"] != null }
            .forEach {
                val relationship = it["relOfRel"] as Relationship
                val innerRelType = (it["relOfRelType"] as String)
                val innerTargetEntityId = (it["relOfRelObject"] as Entity).id

                val innerRelationship = mapOf(
                    JSONLD_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                    NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(JSONLD_ID to innerTargetEntityId)
                )

                val innerRelationshipValues = relationship.serializeCoreProperties(includeSysAttrs)
                innerRelationshipValues.putAll(innerRelationship)
                val expandedInnerRelationshipType =
                    expandRelationshipType(mapOf(innerRelType to relationshipValue), contexts)

                relationshipValues[expandedInnerRelationshipType] = innerRelationshipValues
            }

        return Pair(primaryRelType, relationshipValues)
    }

    fun getSerializedEntityById(entityId: URI): String? {
        return getFullEntityById(entityId, true)?.let {
            serializeObject(it.compact())
        }
    }

    /** @param includeSysAttrs true if createdAt and modifiedAt have to be displayed in the entity
     */
    fun searchEntities(
        type: String,
        query: List<String>,
        contextLink: String,
        includeSysAttrs: Boolean
    ): List<JsonLdEntity> =
        searchEntities(type, query, listOf(contextLink), includeSysAttrs)

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
        type: String,
        query: List<String>,
        contexts: List<String>,
        includeSysAttrs: Boolean
    ): List<JsonLdEntity> {
        val expandedType = expandJsonLdKey(type, contexts)!!

        val queryCriteria = query
            .map {
                val splitted = extractComparaisonParametersFromQuery(it)
                val expandedParam =
                    if (splitted[2].startsWith("urn:"))
                        splitted[0].toRelationshipTypeName()
                    else
                        expandJsonLdKey(splitted[0], contexts)!!
                Triple(expandedParam, splitted[1], splitted[2])
            }
            .partition {
                it.third.startsWith("urn:")
            }
        return neo4jRepository.getEntitiesByTypeAndQuery(expandedType, queryCriteria)
            .map { getFullEntityById(it, includeSysAttrs) }
            .filterNotNull()
    }

    // TODO send append events to Kafka
    @Transactional
    fun appendEntityAttributes(
        entityId: URI,
        attributes: List<NgsiLdAttribute>,
        disallowOverwrite: Boolean
    ): UpdateResult {
        val updateStatuses = attributes
            .flatMap { ngsiLdAttribute ->
                logger.debug("Fragment is of type $ngsiLdAttribute")
                when (ngsiLdAttribute) {
                    is NgsiLdRelationship -> appendEntityRelationship(entityId, ngsiLdAttribute, disallowOverwrite)
                    is NgsiLdProperty -> ngsiLdAttribute.instances.map { ngsiLdPropertyInstance ->
                        appendEntityProperty(entityId, ngsiLdAttribute, ngsiLdPropertyInstance, disallowOverwrite)
                    }
                    is NgsiLdGeoProperty -> appendEntityGeoProperty(entityId, ngsiLdAttribute, disallowOverwrite)
                    // TODO we should avoid this fake else
                    else -> listOf(Triple("", false, "Unknown attribute type $ngsiLdAttribute"))
                }
            }
            .toList()

        // update modifiedAt in entity if at least one attribute has been added
        if (updateStatuses.isNotEmpty())
            neo4jRepository.updateEntityModifiedDate(entityId)

        val updated = updateStatuses.filter { it.second }.map { it.first }
        val notUpdated = updateStatuses.filter { !it.second }.map { NotUpdatedDetails(it.first, it.third!!) }

        return UpdateResult(updated, notUpdated)
    }

    fun appendEntityRelationship(
        entityId: URI,
        ngsiLdRelationship: NgsiLdRelationship,
        disallowOverwrite: Boolean
    ): List<Triple<String, Boolean, String?>> {
        val relationshipTypeName = ngsiLdRelationship.name.extractShortTypeFromExpanded()
        return if (!neo4jRepository.hasRelationshipOfType(
                EntitySubjectNode(entityId),
                relationshipTypeName
            )
        ) {
            createEntityRelationship(
                entityId,
                ngsiLdRelationship.name,
                ngsiLdRelationship.instances[0],
                ngsiLdRelationship.instances[0].objectId
            )
            listOf(Triple(ngsiLdRelationship.name, true, null))
        } else if (disallowOverwrite) {
            logger.info(
                "Relationship $relationshipTypeName already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
            listOf(
                Triple(
                    ngsiLdRelationship.name,
                    false,
                    "Relationship $relationshipTypeName already exists on $entityId " +
                        "and overwrite is not allowed, ignoring"
                )
            )
        } else {
            neo4jRepository.deleteEntityRelationship(
                EntitySubjectNode(entityId),
                relationshipTypeName
            )
            createEntityRelationship(
                entityId,
                ngsiLdRelationship.name,
                ngsiLdRelationship.instances[0],
                ngsiLdRelationship.instances[0].objectId
            )
            listOf(Triple(ngsiLdRelationship.name, true, null))
        }
    }

    fun appendEntityProperty(
        entityId: URI,
        ngsiLdProperty: NgsiLdProperty,
        ngsiLdPropertyInstance: NgsiLdPropertyInstance,
        disallowOverwrite: Boolean
    ): Triple<String, Boolean, String?> {
        return if (!neo4jRepository.hasPropertyInstance(
            EntitySubjectNode(entityId),
            ngsiLdProperty.name,
            ngsiLdPropertyInstance.datasetId
        )
        ) {
            createEntityProperty(entityId, ngsiLdProperty.name, ngsiLdPropertyInstance)
            Triple(ngsiLdProperty.name, true, null)
        } else if (disallowOverwrite) {
            logger.info(
                "Property ${ngsiLdProperty.name} already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
            Triple(
                ngsiLdProperty.name,
                false,
                "Property ${ngsiLdProperty.name} already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
        } else {
            neo4jRepository.deleteEntityProperty(EntitySubjectNode(entityId), ngsiLdProperty.name)
            createEntityProperty(entityId, ngsiLdProperty.name, ngsiLdPropertyInstance)
            Triple(ngsiLdProperty.name, true, null)
        }
    }

    fun appendEntityGeoProperty(
        entityId: URI,
        ngsiLdGeoProperty: NgsiLdGeoProperty,
        disallowOverwrite: Boolean
    ): List<Triple<String, Boolean, String?>> {
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
            listOf(Triple(ngsiLdGeoProperty.name, true, null))
        } else if (disallowOverwrite) {
            logger.info(
                "GeoProperty ${ngsiLdGeoProperty.name} already exists on $entityId " +
                    "and overwrite is not allowed, ignoring"
            )
            listOf(
                Triple(
                    ngsiLdGeoProperty.name,
                    false,
                    "GeoProperty ${ngsiLdGeoProperty.name} already exists on $entityId " +
                        "and overwrite is not allowed, ignoring"
                )
            )
        } else {
            updateLocationPropertyOfEntity(
                entityId,
                ngsiLdGeoProperty.name,
                ngsiLdGeoProperty.instances[0]
            )
            listOf(Triple(ngsiLdGeoProperty.name, true, null))
        }
    }

    @Transactional
    fun updateEntityAttribute(id: URI, attribute: String, payload: String, contexts: List<String>): Int {
        val expandedAttributeName = expandJsonLdKey(attribute, contexts)!!
        val attributeValue = parseJsonLdFragment(payload)["value"]!!
        return neo4jRepository.updateEntityAttribute(id, expandedAttributeName, attributeValue)
    }

    @Transactional
    fun updateEntityAttributes(id: URI, attributes: List<NgsiLdAttribute>): UpdateResult {
        val updatedAttributes = mutableListOf<String>()
        val notUpdatedAttributes = mutableListOf<NotUpdatedDetails>()

        attributes.forEach { ngsiLdAttribute ->
            val shortAttributeName = ngsiLdAttribute.compactName
            try {
                logger.debug("Trying to update attribute ${ngsiLdAttribute.name} of type $ngsiLdAttribute")
                if (ngsiLdAttribute is NgsiLdRelationship) {
                    if (neo4jRepository.hasRelationshipOfType(
                        EntitySubjectNode(id),
                        ngsiLdAttribute.name.toRelationshipTypeName()
                    )
                    ) {
                        deleteEntityAttribute(id, ngsiLdAttribute.name)
                        // TODO multi-attribute support
                        createEntityRelationship(
                            id,
                            ngsiLdAttribute.name,
                            ngsiLdAttribute.instances[0],
                            ngsiLdAttribute.instances[0].objectId
                        )
                        updatedAttributes.add(ngsiLdAttribute.name)
                    } else
                        notUpdatedAttributes.add(NotUpdatedDetails(ngsiLdAttribute.name, "Relationship does not exist"))
                } else if (ngsiLdAttribute is NgsiLdProperty) {
                    val datasetId = ngsiLdAttribute.instances[0].datasetId
                    if (neo4jRepository.hasPropertyInstance(EntitySubjectNode(id), ngsiLdAttribute.name, datasetId)) {
                        updateEntityAttributeInstance(id, ngsiLdAttribute.name, ngsiLdAttribute.instances[0])
                        updatedAttributes.add(ngsiLdAttribute.name)
                    } else {
                        val message = if (datasetId != null)
                            "Property (datasetId: $datasetId) does not exist"
                        else
                            "Property (default instance) does not exist"
                        notUpdatedAttributes.add(NotUpdatedDetails(ngsiLdAttribute.name, message))
                    }
                } else if (ngsiLdAttribute is NgsiLdGeoProperty) {
                    if (neo4jRepository.hasGeoPropertyOfName(EntitySubjectNode(id), shortAttributeName)) {
                        updateLocationPropertyOfEntity(id, ngsiLdAttribute.name, ngsiLdAttribute.instances[0])
                        updatedAttributes.add(ngsiLdAttribute.name)
                    } else
                        notUpdatedAttributes.add(NotUpdatedDetails(ngsiLdAttribute.name, "GeoProperty does not exist"))
                }
            } catch (e: BadRequestDataException) {
                notUpdatedAttributes.add(
                    NotUpdatedDetails(ngsiLdAttribute.name, e.message)
                )
            }
        }

        return UpdateResult(updatedAttributes, notUpdatedAttributes)
    }

    internal fun updateLocationPropertyOfEntity(
        entityId: URI,
        propertyKey: String,
        ngsiLdGeoPropertyInstance: NgsiLdGeoPropertyInstance
    ) {
        logger.debug("Geo property $propertyKey has values ${ngsiLdGeoPropertyInstance.coordinates}")
        // TODO : point is not part of the NGSI-LD core context (https://redmine.eglobalmark.com/issues/869)
        if (ngsiLdGeoPropertyInstance.geoPropertyType == "Point") {
            neo4jRepository.updateLocationPropertyOfEntity(
                entityId,
                Pair(
                    ngsiLdGeoPropertyInstance.coordinates[0] as Double,
                    ngsiLdGeoPropertyInstance.coordinates[1] as Double
                )
            )
        } else {
            throw BadRequestDataException("Unsupported geometry type : ${ngsiLdGeoPropertyInstance.geoPropertyType}")
        }
    }

    @Transactional
    fun deleteEntity(entityId: URI): Pair<Int, Int> {
        val nodesAndRelationshipsDeleted = neo4jRepository.deleteEntity(entityId)
        publishDeletionEvent(entityId)
        return nodesAndRelationshipsDeleted
    }

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

    @Transactional
    fun updateEntityLastMeasure(observation: Observation) {
        val observingEntity =
            neo4jRepository.getObservingSensorEntity(observation.observedBy, EGM_VENDOR_ID, observation.attributeName)
        if (observingEntity == null) {
            logger.warn(
                "Unable to find observing entity ${observation.observedBy} " +
                    "for property ${observation.attributeName}"
            )
            return
        }
        // Find the previous observation of the same unit for the given sensor, then create or update it
        val observedByRelationshipType = EGM_OBSERVED_BY.extractShortTypeFromExpanded()
        val observedProperty = neo4jRepository.getObservedProperty(observingEntity.id, observedByRelationshipType)
        if (observedProperty == null ||
            observedProperty.name.extractShortTypeFromExpanded() != observation.attributeName
        ) {
            logger.warn(
                "Found no property named ${observation.attributeName} " +
                    "observed by ${observation.observedBy}, ignoring it"
            )
        } else {
            observedProperty.updateValues(observation.unitCode, observation.value, observation.observedAt)
            if (observation.latitude != null && observation.longitude != null) {
                val observedEntity = neo4jRepository.getEntityByProperty(observedProperty)
                observedEntity.location = GeographicPoint2d(observation.latitude!!, observation.longitude!!)
                entityRepository.save(observedEntity)
            }
            propertyRepository.save(observedProperty)

            val entity = neo4jRepository.getEntityByProperty(observedProperty)
            val rawProperty = entityRepository.getEntitySpecificProperty(
                entity.id.toString(), observedProperty.id.toString()
            )
            val propertyFragment = buildPropertyFragment(rawProperty, entity.contexts, true)
            val propertyPayload = compactAndStringifyFragment(
                expandJsonLdKey(propertyFragment.first, entity.contexts)!!,
                propertyFragment.second, entity.contexts
            )
            val entityEvent = EntityEvent(
                operationType = EventType.UPDATE,
                entityId = entity.id,
                entityType = entity.type[0].extractShortTypeFromExpanded(),
                payload = propertyPayload
            )
            applicationEventPublisher.publishEvent(entityEvent)
        }
    }
}
