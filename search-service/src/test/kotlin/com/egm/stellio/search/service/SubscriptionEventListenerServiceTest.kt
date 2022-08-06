package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    classes = [SubscriptionEventListenerService::class]
)
@ActiveProfiles("test")
class SubscriptionEventListenerServiceTest {

    @Autowired
    private lateinit var subscriptionEventListenerService: SubscriptionEventListenerService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @Test
    fun `it should parse a subscription and create a temporal entity reference`() = runTest {
        val subscriptionEvent = loadSampleData("events/subscription/subscriptionCreateEvent.jsonld")

        coEvery { temporalEntityAttributeService.create(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.setAdminRoleOnEntity(any(), any()) } returns Unit.right()

        subscriptionEventListenerService.dispatchSubscriptionMessage(subscriptionEvent)

        coVerify {
            temporalEntityAttributeService.create(
                match { entityTemporalProperty ->
                    entityTemporalProperty.attributeName == "https://uri.etsi.org/ngsi-ld/notification" &&
                        entityTemporalProperty.attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY &&
                        entityTemporalProperty.entityId == "urn:ngsi-ld:Subscription:04".toUri() &&
                        entityTemporalProperty.types == listOf("https://uri.etsi.org/ngsi-ld/Subscription")
                }
            )
            entityAccessRightsService.setAdminRoleOnEntity(null, "urn:ngsi-ld:Subscription:04".toUri())
        }
    }

    @Test
    fun `it should parse a notification and create one related observation`() = runTest {
        val temporalEntityAttributeUuid = UUID.randomUUID()
        val notificationEvent = loadSampleData("events/subscription/notificationCreateEvent.jsonld")

        coEvery { temporalEntityAttributeService.getFirstForEntity(any()) } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        subscriptionEventListenerService.dispatchNotificationMessage(notificationEvent)

        coVerify { temporalEntityAttributeService.getFirstForEntity(eq("urn:ngsi-ld:Subscription:1234".toUri())) }
        coVerify {
            attributeInstanceService.create(
                match {
                    it.value == "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTD" &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.payload.matchContent(
                            """
                            {
                                "type": "Notification",
                                "value": "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTD",
                                "observedAt": "2020-03-10T00:00:00Z",
                                "instanceId": "urn:ngsi-ld:Notification:1234"
                            }
                            """.trimIndent()
                        )
                }
            )
        }
    }

    @Test
    fun `it should delete subscription temporal references`() = runTest {
        val subscriptionEvent = loadSampleData("events/subscription/subscriptionDeleteEvent.jsonld")

        coEvery { temporalEntityAttributeService.deleteTemporalEntityReferences(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.removeRolesOnEntity(any()) } returns Unit.right()

        subscriptionEventListenerService.dispatchSubscriptionMessage(subscriptionEvent)

        coVerify {
            temporalEntityAttributeService.deleteTemporalEntityReferences(eq("urn:ngsi-ld:Subscription:04".toUri()))
            entityAccessRightsService.removeRolesOnEntity(eq("urn:ngsi-ld:Subscription:04".toUri()))
        }
    }
}
