package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.PropertyRepository
import com.egm.stellio.entity.repository.RelationshipRepository
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class SubscriptionHandlerService(
    private val entityService: EntityService,
    private val propertyRepository: PropertyRepository,
    private val entityRepository: EntityRepository,
    private val neo4jRepository: Neo4jRepository,
    private val relationshipRepository: RelationshipRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun createSubscriptionEntity(id: String, type: String, properties: Map<String, Any>) {
        if (entityService.exists(id)) {
            logger.warn("Subscription $id already exists")
            return
        }

        val subscription = Entity(
            id = id,
            type = listOf(JsonLdUtils.expandJsonLdKey(type, JsonLdUtils.NGSILD_CORE_CONTEXT)!!)
        )
        properties.forEach {
            val property = propertyRepository.save(
                Property(
                    name = JsonLdUtils.expandJsonLdKey(it.key, JsonLdUtils.NGSILD_CORE_CONTEXT)!!,
                    value = JsonUtils.serializeObject(it.value)
                )
            )
            subscription.properties.add(property)
        }
        subscription.contexts = listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
        entityRepository.save(subscription)
    }

    @Transactional
    fun createNotificationEntity(id: String, type: String, subscriptionId: String, properties: Map<String, Any>) {
        val subscription = entityRepository.findById(subscriptionId)
        if (!subscription.isPresent) {
            logger.warn("Subscription $subscriptionId does not exist")
            return
        }

        val notification = Entity(
            id = id,
            type = listOf(JsonLdUtils.expandJsonLdKey(type, JsonLdUtils.NGSILD_CORE_CONTEXT)!!)
        )
        properties.forEach {
            val property = propertyRepository.save(
                Property(
                    name = JsonLdUtils.expandJsonLdKey(it.key, JsonLdUtils.NGSILD_CORE_CONTEXT)!!,
                    value = JsonUtils.serializeObject(it.value)
                )
            )
            notification.properties.add(property)
        }
        notification.contexts = listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
        entityRepository.save(notification)

        // Find the last notification of the subscription
        val lastNotification = neo4jRepository.getRelationshipTargetOfSubject(
            subscriptionId,
            JsonLdUtils.EGM_RAISED_NOTIFICATION.toRelationshipTypeName()
        )

        // Create relationship between the subscription and the new notification
        if (lastNotification != null) {
            val relationship = neo4jRepository.getRelationshipOfSubject(
                subscriptionId,
                JsonLdUtils.EGM_RAISED_NOTIFICATION.toRelationshipTypeName()
            )
            neo4jRepository.updateRelationshipTargetOfAttribute(
                relationship.id,
                JsonLdUtils.EGM_RAISED_NOTIFICATION.toRelationshipTypeName(),
                lastNotification.id,
                notification.id
            )
            relationshipRepository.save(relationship)
            entityService.deleteEntity(lastNotification.id)
        } else {
            val rawRelationship = Relationship(
                type = listOf(JsonLdUtils.EGM_RAISED_NOTIFICATION)
            )

            neo4jRepository.createRelationshipOfSubject(
                EntitySubjectNode(subscription.get().id),
                rawRelationship,
                notification.id
            )
        }
    }
}
