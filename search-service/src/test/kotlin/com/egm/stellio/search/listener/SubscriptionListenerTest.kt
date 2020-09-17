package com.egm.stellio.search.listener

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.shared.util.loadSampleData
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [SubscriptionListener::class])
@ActiveProfiles("test")
class SubscriptionListenerTest {

    @Autowired
    private lateinit var subscriptionListener: SubscriptionListener

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @Test
    fun `it should parse a subscription and create a temporal entity reference`() {
        val subscription = loadSampleData("subscription_event.jsonld")

        every { temporalEntityAttributeService.create(any()) } returns Mono.just(1)

        subscriptionListener.processSubscription(subscription)

        verify {
            temporalEntityAttributeService.create(
                match { entityTemporalProperty ->
                    entityTemporalProperty.attributeName == "https://uri.etsi.org/ngsi-ld/notification" &&
                        entityTemporalProperty.attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY &&
                        entityTemporalProperty.entityId == "urn:ngsi-ld:Subscription:1234".toUri() &&
                        entityTemporalProperty.type == "https://uri.etsi.org/ngsi-ld/Subscription"
                }
            )
        }
        confirmVerified(temporalEntityAttributeService)
    }

    @Test
    fun `it should parse a notification and create one related observation`() {
        val temporalEntityAttributeUuid = UUID.randomUUID()
        val notification = loadSampleData("notification_event.jsonld")

        every { temporalEntityAttributeService.getFirstForEntity(any()) } returns Mono.just(temporalEntityAttributeUuid)
        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        subscriptionListener.processNotification(notification)

        verify { temporalEntityAttributeService.getFirstForEntity(eq("urn:ngsi-ld:Subscription:1234".toUri())) }
        verify {
            attributeInstanceService.create(
                match {
                    it.value == "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTD" &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid
                }
            )
        }
        confirmVerified(temporalEntityAttributeService)
        confirmVerified(attributeInstanceService)
    }
}
