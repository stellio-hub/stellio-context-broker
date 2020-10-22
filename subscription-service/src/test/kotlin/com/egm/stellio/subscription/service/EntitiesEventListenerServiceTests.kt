package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntityCreateEvent
import com.ninjasquad.springmockk.MockkBean
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(classes = [EntitiesEventListenerService::class])
@ActiveProfiles("test")
class EntitiesEventListenerServiceTests {

    @Autowired
    private lateinit var entitiesEventListenerService: EntitiesEventListenerService

    @MockkBean
    private lateinit var notificationService: NotificationService

    @Test
    fun `it should parse an event of type CREATE_ENTITY`() {
        val eventPayload = ClassPathResource("/ngsild/events/entityCreateEvent.jsonld")
        val parsedEvent = entitiesEventListenerService.parseEntitiesEvent(
            eventPayload.inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is EntityCreateEvent)
    }
}
