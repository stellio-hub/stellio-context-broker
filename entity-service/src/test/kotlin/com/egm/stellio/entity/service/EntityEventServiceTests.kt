package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.messaging.Message
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(classes = [EntityEventService::class])
@ActiveProfiles("test")
class EntityEventServiceTests {

    @Autowired
    private lateinit var entityEventService: EntityEventService

    @MockkBean(relaxed = true)
    private lateinit var resolver: BinderAwareChannelResolver

    private val entityPayload =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": {
                "type": "Property",
                "value": "Mercedes"
            },
            "@context": [
                "http://example.org/ngsi-ld/latest/vehicle.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ]
        }
        """.trimIndent()

    @Test
    fun `it should publish an event of type ENTITY_CREATE`() {
        val event = EntityCreateEvent("urn:ngsi-ld:Vehicle:A4567".toUri(), entityPayload)
        val message = slot<Message<String>>()

        every {
            resolver.resolveDestination(any()).send(capture(message))
        } returns true

        entityEventService.publishEntityEvent(event, "Vehicle")

        verify { resolver.resolveDestination("cim.entity.Vehicle") }

        Assertions.assertTrue(
            message.captured.payload.matchContent(
                loadSampleData("events/entityCreateEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should publish an event of type ENTITY_DELETE`() {
        val event = EntityDeleteEvent("urn:ngsi-ld:Bus:A4567".toUri())
        val message = slot<Message<String>>()

        every {
            resolver.resolveDestination(any()).send(capture(message))
        } returns true

        entityEventService.publishEntityEvent(event, "Bus")

        verify { resolver.resolveDestination("cim.entity.Bus") }

        Assertions.assertTrue(
            message.captured.payload.matchContent(
                loadSampleData("events/entityDeleteEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should publish an event of type ATTRIBUTE_REPLACE`() {
        val event = AttributeReplaceEvent(
            "urn:ngsi-ld:Bus:A4567".toUri(),
            "color",
            "urn:ngsi-ld:Dataset:color:1".toUri(),
            "{ \"type\": \"Property\", \"value\": \"red\" }",
            "updatedEntity",
            listOf(NGSILD_CORE_CONTEXT)
        )

        val message = slot<Message<String>>()

        every {
            resolver.resolveDestination(any()).send(capture(message))
        } returns true

        entityEventService.publishEntityEvent(event, "Bus")

        verify { resolver.resolveDestination("cim.entity.Bus") }

        Assertions.assertTrue(
            message.captured.payload.matchContent(
                loadSampleData("events/attributeReplaceEvent.jsonld")
            )
        )
    }
}
