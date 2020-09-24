package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.PropertyRepository
import com.egm.stellio.entity.repository.RelationshipRepository
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [SubscriptionHandlerService::class])
@ActiveProfiles("test")
class SubscriptionHandlerServiceTests {

    @Autowired
    private lateinit var subscriptionHandlerService: SubscriptionHandlerService

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    @MockkBean(relaxed = true)
    private lateinit var entityRepository: EntityRepository

    @MockkBean
    private lateinit var propertyRepository: PropertyRepository

    @MockkBean
    private lateinit var relationshipRepository: RelationshipRepository

    @Test
    fun `it should create a new subscription`() {
        val subscriptionId = "urn:ngsi-ld:Subscription:04".toUri()
        val subscriptionType = "Subscription"
        val properties = mapOf(
            "q" to "foodQuantity<150;foodName=='dietary fibres'"
        )
        val mockkedSubscription = mockkClass(Entity::class)
        val mockkedProperty = mockkClass(Property::class)

        every { entityService.exists(any()) } returns false
        every { propertyRepository.save<Property>(any()) } returns mockkedProperty
        every { entityRepository.save<Entity>(any()) } returns mockkedSubscription

        subscriptionHandlerService.createSubscriptionEntity(subscriptionId, subscriptionType, properties)

        verify { entityService.exists(eq(subscriptionId)) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }

        confirmVerified()
    }

    @Test
    fun `it should not create subscription if exists`() {
        val subscriptionId = "urn:ngsi-ld:Subscription:04".toUri()
        val subscriptionType = "Subscription"
        val properties = mapOf(
            "q" to "foodQuantity<150;foodName=='dietary fibres'"
        )

        every { entityService.exists(any()) } returns true

        subscriptionHandlerService.createSubscriptionEntity(subscriptionId, subscriptionType, properties)

        verify { entityService.exists(eq(subscriptionId)) }
        verify { propertyRepository wasNot Called }

        confirmVerified()
    }

    @Test
    fun `it should create a new notification and add a relationship to the subscription`() {
        val subscriptionId = "urn:ngsi-ld:Subscription:1234".toUri()
        val notificationId = "urn:ngsi-ld:Notification:1234".toUri()
        val relationshipId = "urn:ngsi-ld:Relationship:7d0ea653-c932-43cc-aa41-29ac1c77c610".toUri()
        val notificationType = "Notification"
        val properties = mapOf(
            "notifiedAt" to "2020-03-10T00:00:00Z"
        )
        val mockkedSubscription = mockkClass(Entity::class)
        val mockkedNotification = mockkClass(Entity::class)
        val mockkedProperty = mockkClass(Property::class)

        every { entityRepository.getEntityCoreById(any()) } returns mockkedSubscription
        every { propertyRepository.save<Property>(any()) } returns mockkedProperty
        every { entityRepository.save<Entity>(any()) } returns mockkedNotification
        every { neo4jRepository.getRelationshipTargetOfSubject(any(), any()) } returns null
        every { mockkedSubscription.id } returns subscriptionId
        every { neo4jRepository.createRelationshipOfSubject(any(), any(), any()) } returns relationshipId

        subscriptionHandlerService.createNotificationEntity(
            notificationId, notificationType, subscriptionId, properties
        )

        verify { entityRepository.getEntityCoreById(eq(subscriptionId.toString())) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }
        verify {
            neo4jRepository.getRelationshipTargetOfSubject(
                subscriptionId,
                JsonLdUtils.EGM_RAISED_NOTIFICATION.toRelationshipTypeName()
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should remove the last notification create a new one and update the relationship to the subscription`() {
        val subscriptionId = "urn:ngsi-ld:Subscription:1234".toUri()
        val notificationId = "urn:ngsi-ld:Notification:1234".toUri()
        val lastNotificationId = "urn:ngsi-ld:Notification:1233".toUri()
        val relationshipId = "urn:ngsi-ld:Relationship:7d0ea653-c932-43cc-aa41-29ac1c77c610".toUri()
        val notificationType = "Notification"
        val properties = mapOf(
            "notifiedAt" to "2020-03-10T00:00:00Z"
        )
        val mockkedSubscription = mockkClass(Entity::class)
        val mockkedNotification = mockkClass(Entity::class)
        val mockkedLastNotification = mockkClass(Entity::class)
        val mockkedProperty = mockkClass(Property::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { entityRepository.getEntityCoreById(any()) } returns mockkedSubscription
        every { propertyRepository.save<Property>(any()) } returns mockkedProperty
        every { entityRepository.save<Entity>(any()) } returns mockkedNotification
        every { neo4jRepository.getRelationshipTargetOfSubject(any(), any()) } returns mockkedLastNotification
        every { neo4jRepository.getRelationshipOfSubject(any(), any()) } returns mockkedRelationship
        every { mockkedRelationship.id } returns relationshipId
        every { mockkedNotification.id } returns notificationId
        every { mockkedLastNotification.id } returns lastNotificationId
        every { neo4jRepository.updateRelationshipTargetOfAttribute(any(), any(), any(), any()) } returns 1
        every { relationshipRepository.save<Relationship>(any()) } returns mockkedRelationship
        every { entityService.deleteEntity(any()) } returns Pair(1, 1)

        subscriptionHandlerService.createNotificationEntity(
            notificationId, notificationType, subscriptionId, properties
        )

        verify { entityRepository.getEntityCoreById(eq(subscriptionId.toString())) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }
        verify {
            neo4jRepository.getRelationshipTargetOfSubject(
                subscriptionId,
                JsonLdUtils.EGM_RAISED_NOTIFICATION.toRelationshipTypeName()
            )
        }
        verify {
            neo4jRepository.getRelationshipOfSubject(
                subscriptionId,
                JsonLdUtils.EGM_RAISED_NOTIFICATION.toRelationshipTypeName()
            )
        }
        verify {
            neo4jRepository.updateRelationshipTargetOfAttribute(
                relationshipId,
                JsonLdUtils.EGM_RAISED_NOTIFICATION.toRelationshipTypeName(),
                lastNotificationId,
                notificationId
            )
        }
        verify { relationshipRepository.save(any<Relationship>()) }
        every { entityService.deleteEntity(lastNotificationId) }

        confirmVerified()
    }

    @Test
    fun `it should not create notification if the related subscription does not exist`() {
        val notificationId = "urn:ngsi-ld:Notification:1234".toUri()
        val notificationType = "Notification"
        val subscriptionId = "urn:ngsi-ld:Subscription:1234".toUri()
        val properties = mapOf(
            "notifiedAt" to "2020-03-10T00:00:00Z"
        )

        every { entityRepository.getEntityCoreById(any()) } returns null

        subscriptionHandlerService.createNotificationEntity(
            notificationId, notificationType, subscriptionId, properties
        )

        verify { entityRepository.getEntityCoreById(eq(subscriptionId.toString())) }
        verify { propertyRepository wasNot Called }

        confirmVerified()
    }
}
