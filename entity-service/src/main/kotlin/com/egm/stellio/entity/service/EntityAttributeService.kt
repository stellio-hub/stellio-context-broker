package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.repository.AttributeSubjectNode
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.PropertyRepository
import com.egm.stellio.entity.repository.RelationshipRepository
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import org.slf4j.LoggerFactory
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

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun partialUpdateEntityAttribute(
        entityId: URI,
        expandedPayload: Map<String, List<Map<String, List<Any>>>>,
        contexts: List<String>
    ): UpdateResult {
        val expandedAttributeName = expandedPayload.keys.first()
        val attributeValues = expandedPayload.values.first()

        logger.debug("Updating attribute $expandedAttributeName of entity $entityId with values: $attributeValues")

        val updateResult = attributeValues.map { attributeInstanceValues ->
            val datasetId = attributeInstanceValues.getDatasetId()
            when {
                neo4jRepository.hasRelationshipInstance(
                    EntitySubjectNode(entityId),
                    expandedAttributeName.toRelationshipTypeName(),
                    datasetId
                ) ->
                    partialUpdateEntityRelationship(
                        entityId,
                        expandedAttributeName.toRelationshipTypeName(),
                        attributeInstanceValues,
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
                        attributeInstanceValues,
                        datasetId,
                        contexts
                    )
                else -> UpdateAttributeResult(
                    expandedAttributeName,
                    datasetId,
                    UpdateOperationResult.IGNORED,
                    "Unknown attribute $expandedAttributeName with datasetId $datasetId in entity $entityId"
                )
            }
        }

        // still throwing an exception to trigger the rollback of the transactions of other instances
        if (updateResult.any { it.updateOperationResult == UpdateOperationResult.FAILED })
            throw InternalErrorException("Partial update operation failed to perform the whole update")

        // update modifiedAt in entity if at least one attribute has been updated
        if (updateResult.any { it.isSuccessfullyUpdated() })
            neo4jRepository.updateEntityModifiedDate(entityId)

        return updateResultFromDetailedResult(updateResult)
    }

    internal fun partialUpdateEntityRelationship(
        entityId: URI,
        relationshipType: String,
        relationshipValues: Map<String, List<Any>>,
        datasetId: URI?,
        contexts: List<String>
    ): UpdateAttributeResult {
        val relationship =
            if (datasetId != null)
                relationshipRepository.getRelationshipOfSubject(entityId, relationshipType, datasetId)
            else
                relationshipRepository.getRelationshipOfSubject(entityId, relationshipType)
        val relationshipUpdates = partialUpdateRelationshipOfAttribute(
            relationship,
            entityId,
            relationshipType,
            datasetId,
            relationshipValues
        )
        val attributesOfRelationshipUpdates = partialUpdateAttributesOfAttribute(
            relationship,
            relationshipValues,
            contexts
        )
        return if (relationshipUpdates && attributesOfRelationshipUpdates)
            UpdateAttributeResult(
                expandJsonLdTerm(relationshipType, contexts)!!,
                datasetId,
                UpdateOperationResult.UPDATED,
                null
            ) else UpdateAttributeResult(
            expandJsonLdTerm(relationshipType, contexts)!!,
            datasetId,
            UpdateOperationResult.FAILED,
            "Partial update operation failed to perform the whole update"
        )
    }

    internal fun partialUpdateEntityProperty(
        entityId: URI,
        expandedPropertyName: String,
        propertyValues: Map<String, List<Any>>,
        datasetId: URI?,
        contexts: List<String>
    ): UpdateAttributeResult {
        val property =
            if (datasetId != null)
                propertyRepository.getPropertyOfSubject(entityId, expandedPropertyName, datasetId)
            else
                propertyRepository.getPropertyOfSubject(entityId, expandedPropertyName)
        val propertyUpdates = partialUpdatePropertyOfAttribute(
            property,
            propertyValues
        )
        val attributesOfPropertyUpdates = partialUpdateAttributesOfAttribute(property, propertyValues, contexts)
        return if (propertyUpdates && attributesOfPropertyUpdates)
            UpdateAttributeResult(
                expandedPropertyName,
                datasetId,
                UpdateOperationResult.UPDATED,
                null
            ) else UpdateAttributeResult(
            expandedPropertyName,
            datasetId,
            UpdateOperationResult.FAILED,
            "Partial update operation failed to perform the whole update"
        )
    }

    internal fun partialUpdateAttributesOfAttribute(
        attribute: Attribute,
        attributeValues: Map<String, List<Any>>,
        contexts: List<String>
    ): Boolean {
        return attributeValues.filterKeys {
            if (attribute is Relationship)
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
                    AttributeSubjectNode(attribute.id()),
                    attributeOfAttributeName.toRelationshipTypeName()
                ) -> {
                    val relationshipOfAttribute = relationshipRepository.getRelationshipOfSubject(
                        attribute.id(),
                        attributeOfAttributeName.toRelationshipTypeName()
                    )
                    partialUpdateRelationshipOfAttribute(
                        relationshipOfAttribute,
                        attribute.id(),
                        attributeOfAttributeName.toRelationshipTypeName(),
                        null,
                        it.value
                    )
                }
                neo4jRepository.hasPropertyInstance(
                    AttributeSubjectNode(attribute.id()),
                    expandJsonLdTerm(attributeOfAttributeName, contexts)!!
                ) -> {
                    val propertyOfAttribute = propertyRepository.getPropertyOfSubject(
                        attribute.id(),
                        expandJsonLdTerm(attributeOfAttributeName, contexts)!!
                    )
                    partialUpdatePropertyOfAttribute(
                        propertyOfAttribute,
                        it.value
                    )
                }
                else -> {
                    if (isAttributeOfType(it.value, JsonLdUtils.NGSILD_RELATIONSHIP_TYPE)) {
                        val ngsiLdRelationship = NgsiLdRelationship(
                            attributeOfAttributeName,
                            listOf(it.value)
                        )
                        entityService.createAttributeRelationships(attribute.id(), listOf(ngsiLdRelationship))
                    } else if (isAttributeOfType(it.value, JsonLdUtils.NGSILD_PROPERTY_TYPE)) {
                        val ngsiLdProperty = NgsiLdProperty(
                            expandJsonLdTerm(attributeOfAttributeName, contexts)!!,
                            listOf(it.value)
                        )
                        entityService.createAttributeProperties(attribute.id(), listOf(ngsiLdProperty))
                    } else false
                }
            }
        }.all { it }
    }

    internal fun partialUpdateRelationshipOfAttribute(
        relationshipOfAttribute: Relationship,
        attributeId: URI,
        relationshipType: String,
        datasetId: URI?,
        relationshipValues: Map<String, List<Any>>
    ): Boolean {
        updateRelationshipTargetOfAttribute(
            attributeId,
            relationshipType,
            datasetId,
            relationshipValues
        )
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
        datasetId: URI?,
        relationshipValues: Map<String, List<Any>>
    ): Boolean =
        JsonLdUtils.extractRelationshipObject(relationshipType, relationshipValues)
            .map { objectId ->
                neo4jRepository.updateRelationshipTargetOfSubject(
                    attributeId,
                    relationshipType,
                    objectId,
                    datasetId
                )
            }
            .fold({ false }, { it })
}
