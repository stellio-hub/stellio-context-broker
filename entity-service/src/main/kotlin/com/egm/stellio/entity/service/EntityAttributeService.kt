package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Attribute
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.entity.repository.AttributeSubjectNode
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.PropertyRepository
import com.egm.stellio.entity.repository.RelationshipRepository
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.NGSILD_PROPERTIES_CORE_MEMBERS
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIPS_CORE_MEMBERS
import com.egm.stellio.shared.model.NgsiLdProperty
import com.egm.stellio.shared.model.NgsiLdRelationship
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.getDatasetId
import com.egm.stellio.shared.model.isAttributeOfType
import com.egm.stellio.shared.util.JsonLdUtils
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Component
class EntityAttributeService(
    private val entityService: EntityService,
    private val neo4jRepository: Neo4jRepository,
    private val propertyRepository: PropertyRepository,
    private val relationshipRepository: RelationshipRepository
) {

    @Transactional
    fun partialUpdateEntityAttribute(
        entityId: URI,
        expandedPayload: Map<String, Map<String, List<Any>>>,
        contexts: List<String>
    ): Boolean {
        val expandedAttributeName = expandedPayload.keys.first()
        val attributeValues = expandedPayload.values.first()
        val datasetId = attributeValues.getDatasetId()
        val partialUpdateResult = when {
            neo4jRepository.hasRelationshipInstance(
                EntitySubjectNode(entityId),
                expandedAttributeName.toRelationshipTypeName(),
                datasetId
            ) ->
                partialUpdateEntityRelationship(
                    entityId,
                    expandedAttributeName.toRelationshipTypeName(),
                    attributeValues,
                    datasetId,
                    contexts
                )
            neo4jRepository.hasPropertyInstance(
                EntitySubjectNode(entityId),
                expandedAttributeName,
                datasetId
            ) ->
                partialUpdateEntityProperty(
                    entityId,
                    expandedAttributeName,
                    attributeValues,
                    datasetId,
                    contexts
                )
            else -> throw ResourceNotFoundException("Unknown attribute $expandedAttributeName in entity $entityId")
        }

        // still throwing an exception to trigger the rollback of the transaction
        if (!partialUpdateResult)
            throw InternalErrorException("Partial update operation failed to perform the whole update")
        else
            return partialUpdateResult
    }

    internal fun partialUpdateEntityRelationship(
        entityId: URI,
        relationshipType: String,
        relationshipValues: Map<String, List<Any>>,
        datasetId: URI?,
        contexts: List<String>
    ): Boolean {
        val relationship = neo4jRepository.getRelationshipOfSubject(entityId, relationshipType)
        val relationshipUpdates = partialUpdateRelationshipOfAttribute(
            relationship,
            entityId,
            relationshipType,
            relationshipValues
        )
        val attributesOfRelationshipUpdates = partialUpdateAttributesOfAttribute(
            relationship,
            relationshipValues,
            contexts
        )
        return relationshipUpdates && attributesOfRelationshipUpdates
    }

    internal fun partialUpdateEntityProperty(
        entityId: URI,
        expandedPropertyName: String,
        propertyValues: Map<String, List<Any>>,
        datasetId: URI?,
        contexts: List<String>
    ): Boolean {
        val property = neo4jRepository.getPropertyOfSubject(entityId, expandedPropertyName, datasetId)
        val propertyUpdates = partialUpdatePropertyOfAttribute(
            property,
            propertyValues
        )
        val attributesOfPropertyUpdates = partialUpdateAttributesOfAttribute(property, propertyValues, contexts)

        return propertyUpdates && attributesOfPropertyUpdates
    }

    internal fun partialUpdateAttributesOfAttribute(
        attribute: Attribute,
        attributeValues: Map<String, List<Any>>,
        contexts: List<String>
    ): Boolean {
        return attributeValues.filterKeys {
            if (attribute.attributeType == "Relationship")
                !NGSILD_RELATIONSHIPS_CORE_MEMBERS.contains(it)
            else
                !NGSILD_PROPERTIES_CORE_MEMBERS.contains(it)
        }.mapValues {
            // attribute attributes cannot be multi-attributes, expand as Map
            JsonLdUtils.expandValueAsMap(it.value)
        }.map {
            val attributeOfAttributeName = it.key
            when {
                neo4jRepository.hasRelationshipInstance(
                    AttributeSubjectNode(attribute.id),
                    attributeOfAttributeName.toRelationshipTypeName()
                ) -> {
                    val relationshipOfAttribute = neo4jRepository.getRelationshipOfSubject(
                        attribute.id,
                        attributeOfAttributeName.toRelationshipTypeName()
                    )
                    partialUpdateRelationshipOfAttribute(
                        relationshipOfAttribute,
                        attribute.id,
                        attributeOfAttributeName.toRelationshipTypeName(),
                        it.value
                    )
                }
                neo4jRepository.hasPropertyInstance(
                    AttributeSubjectNode(attribute.id),
                    JsonLdUtils.expandJsonLdKey(attributeOfAttributeName, contexts)!!
                ) -> {
                    val propertyOfAttribute = neo4jRepository.getPropertyOfSubject(
                        attribute.id,
                        JsonLdUtils.expandJsonLdKey(attributeOfAttributeName, contexts)!!
                    )
                    partialUpdatePropertyOfAttribute(
                        propertyOfAttribute,
                        it.value
                    )
                }
                else -> {
                    if (isAttributeOfType(it.value, JsonLdUtils.NGSILD_RELATIONSHIP_TYPE)) {
                        val ngsiLdRelationship = NgsiLdRelationship(
                            attributeOfAttributeName.toRelationshipTypeName(),
                            listOf(it.value)
                        )
                        entityService.createAttributeRelationships(attribute.id, listOf(ngsiLdRelationship))
                    } else if (isAttributeOfType(it.value, JsonLdUtils.NGSILD_PROPERTY_TYPE)) {
                        val ngsiLdProperty = NgsiLdProperty(
                            JsonLdUtils.expandJsonLdKey(attributeOfAttributeName, contexts)!!,
                            listOf(it.value)
                        )
                        entityService.createAttributeProperties(attribute.id, listOf(ngsiLdProperty))
                    } else false
                }
            }
        }.all { it }
    }

    internal fun partialUpdateRelationshipOfAttribute(
        relationshipOfAttribute: Relationship,
        attributeId: URI,
        relationshipType: String,
        relationshipValues: Map<String, List<Any>>
    ): Boolean {
        updateRelationshipTargetOfAttribute(attributeId, relationshipType, relationshipValues)
        val updatedRelationship = relationshipOfAttribute.updateValues(relationshipValues)
        relationshipRepository.save(updatedRelationship)
        return true
    }

    internal fun partialUpdatePropertyOfAttribute(
        propertyOfAttribute: Property,
        propertyValues: Map<String, List<Any>>
    ): Boolean {
        val updatedProperty = propertyOfAttribute.updateValues(propertyValues)
        propertyRepository.save(updatedProperty)
        return true
    }

    internal fun updateRelationshipTargetOfAttribute(
        attributeId: URI,
        relationshipType: String,
        relationshipValues: Map<String, List<Any>>
    ): Boolean =
        JsonLdUtils.extractRelationshipObject(relationshipType, relationshipValues)
            .map { objectId ->
                neo4jRepository.updateRelationshipTargetOfSubject(
                    attributeId,
                    relationshipType,
                    objectId
                )
            }
            .fold({ false }, { it })
}
