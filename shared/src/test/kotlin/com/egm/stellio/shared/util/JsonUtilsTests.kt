package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.util.JsonUtils.parseEntitiesEvent
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.core.io.ClassPathResource

class JsonUtilsTests {

    @Test
    fun `it should parse an event of type CREATE_ENTITY`() {
        val eventPayload = ClassPathResource("/ngsild/events/entityCreateEvent.jsonld")
        val parsedEvent = parseEntitiesEvent(
            eventPayload.inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is EntityCreateEvent)
    }
}
