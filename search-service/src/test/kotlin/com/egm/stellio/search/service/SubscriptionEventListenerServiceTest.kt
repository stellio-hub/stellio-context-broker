package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalEntityAttribute.AttributeValueType
import com.egm.stellio.shared.util.ExpandedAttributePayloadEntry
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NOTIFICATION_ATTR_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_PROPERTY
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockkClass
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
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @Test
    fun `it should parse a subscription and create a temporal entity reference`() = runTest {
        val subscriptionEvent = loadSampleData("events/subscription/subscriptionCreateEvent.jsonld")

        coEvery {
            entityPayloadService.createEntityPayload(any(), any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery { temporalEntityAttributeService.create(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.setAdminRoleOnEntity(any(), any()) } returns Unit.right()

        subscriptionEventListenerService.dispatchSubscriptionMessage(subscriptionEvent)

        coVerify {
            entityPayloadService.createEntityPayload(
                eq("urn:ngsi-ld:Subscription:04".toUri()),
                listOf(NGSILD_SUBSCRIPTION_PROPERTY),
                isNull(true),
                any(),
                listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT)
            )
            temporalEntityAttributeService.create(
                match { entityTemporalProperty ->
                    entityTemporalProperty.attributeName == NGSILD_NOTIFICATION_ATTR_PROPERTY &&
                        entityTemporalProperty.attributeValueType == AttributeValueType.STRING &&
                        entityTemporalProperty.entityId == "urn:ngsi-ld:Subscription:04".toUri()
                }
            )
            entityAccessRightsService.setAdminRoleOnEntity(
                eq("1343760C-9375-4E3F-B6C1-8A845340EB59"),
                eq("urn:ngsi-ld:Subscription:04".toUri())
            )
        }
    }

    @Test
    fun `it should parse a notification and create one related observation`() = runTest {
        val temporalEntityAttributeUuid = UUID.randomUUID()
        val notificationEvent = loadSampleData("events/subscription/notificationCreateEvent.jsonld")

        coEvery {
            temporalEntityAttributeService.getForEntity(any(), any())
        } returns listOf(
            mockkClass(TemporalEntityAttribute::class) {
                every { id } returns temporalEntityAttributeUuid
            }
        )
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery {
            temporalEntityAttributeService.updateStatus(any(), any(), any<ExpandedAttributePayloadEntry>())
        } returns Unit.right()

        subscriptionEventListenerService.dispatchNotificationMessage(notificationEvent)

        coVerify {
            temporalEntityAttributeService.getForEntity(eq("urn:ngsi-ld:Subscription:1234".toUri()), emptySet())
            attributeInstanceService.create(
                match {
                    it.value == "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTD" &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.payload.matchContent(
                            """
                            {
                                "@type":["https://uri.etsi.org/ngsi-ld/Property"],
                                "https://uri.etsi.org/ngsi-ld/hasValue":[{
                                    "@value":"urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTD"
                                }],
                                "https://uri.etsi.org/ngsi-ld/observedAt":[{
                                    "@type":"https://uri.etsi.org/ngsi-ld/DateTime",
                                    "@value":"2020-03-10T00:00:00Z"
                                }],
                                "https://uri.etsi.org/ngsi-ld/instanceId":[{
                                    "@id":"urn:ngsi-ld:Notification:1234"
                                }]
                            }
                            """.trimIndent()
                        )
                }
            )
            temporalEntityAttributeService.updateStatus(
                eq(temporalEntityAttributeUuid),
                any(),
                any<ExpandedAttributePayloadEntry>()
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
