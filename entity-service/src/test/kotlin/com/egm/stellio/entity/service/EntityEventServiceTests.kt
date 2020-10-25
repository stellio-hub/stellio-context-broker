package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(classes = [EntityEventService::class])
@ActiveProfiles("test")
class EntityEventServiceTests {

    @Autowired
    private lateinit var entityEventService: EntityEventService

    @MockkBean(relaxed = true)
    private lateinit var resolver: BinderAwareChannelResolver

    @Test
    fun `it should publish an event of type ENTITY_CREATE`() {
        val event = EntityCreateEvent("urn:ngsi-ld:Vehicle:A4567".toUri(), "operationPayload")
        every {
            resolver.resolveDestination(any()).send(any())
        } returns true

        entityEventService.publishEntityEvent(event, "Vehicle")

        verify { resolver.resolveDestination("cim.entity.Vehicle") }
    }
}
