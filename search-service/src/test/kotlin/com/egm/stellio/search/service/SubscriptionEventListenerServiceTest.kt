package com.egm.stellio.search.service

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import java.util.UUID

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
    fun `it should parse a subscription and create a temporal entity reference`() {
        val subscriptionEvent = loadSampleData("events/subscription/subscriptionCreateEvent.jsonld")

        every { temporalEntityAttributeService.create(any()) } answers { Mono.just(1) }
        every { entityAccessRightsService.setAdminRoleOnEntity(any(), any()) } answers { Mono.just(1) }

        subscriptionEventListenerService.processSubscription(subscriptionEvent)

        verify {
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
        confirmVerified(temporalEntityAttributeService, entityAccessRightsService)
    }

    @Test
    fun `it should parse a notification and create one related observation`() {
        val temporalEntityAttributeUuid = UUID.randomUUID()
        val notificationEvent = loadSampleData("events/subscription/notificationCreateEvent.jsonld")

        every {
            temporalEntityAttributeService.getFirstForEntity(any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

        subscriptionEventListenerService.processNotification(notificationEvent)

        verify { temporalEntityAttributeService.getFirstForEntity(eq("urn:ngsi-ld:Subscription:1234".toUri())) }
        verify {
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
        confirmVerified(temporalEntityAttributeService, attributeInstanceService)
    }

    @Test
    fun `it should delete subscription temporal references`() {
        val subscriptionEvent = loadSampleData("events/subscription/subscriptionDeleteEvent.jsonld")

        every { temporalEntityAttributeService.deleteTemporalEntityReferences(any()) } answers { Mono.just(10) }
        every { entityAccessRightsService.removeRolesOnEntity(any()) } answers { Mono.just(1) }

        subscriptionEventListenerService.processSubscription(subscriptionEvent)

        verify {
            temporalEntityAttributeService.deleteTemporalEntityReferences(eq("urn:ngsi-ld:Subscription:04".toUri()))
            entityAccessRightsService.removeRolesOnEntity(eq("urn:ngsi-ld:Subscription:04".toUri()))
        }

        confirmVerified(temporalEntityAttributeService, entityAccessRightsService)
    }
}
