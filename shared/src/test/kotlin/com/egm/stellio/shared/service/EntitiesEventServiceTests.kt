package com.egm.stellio.shared.service

import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.slot
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.messaging.Message
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntitiesEventService::class])
@ActiveProfiles("test")
class EntitiesEventServiceTests {

    @Autowired
    private lateinit var entitiesEventService: EntitiesEventService

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
    fun `it should publish an event of type CREATE_ENTITY`() {
        val event = EntityCreateEvent("urn:ngsi-ld:Vehicle:A4567".toUri(), entityPayload)
        val message = slot<Message<String>>()

        every {
            resolver.resolveDestination(any()).send(capture(message))
        } returns true

        entitiesEventService.publishEntityServiceEvent(event, "Vehicle")

        Assertions.assertTrue(message.captured.payload.matchContent(loadSampleData("entityCreateEvent.jsonld")))
    }
}
