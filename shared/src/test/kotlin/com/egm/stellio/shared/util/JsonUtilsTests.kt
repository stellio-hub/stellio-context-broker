package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityUpdateEvent
import com.egm.stellio.shared.util.JsonUtils.parseEntitiesEvent
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.core.io.ClassPathResource

class JsonUtilsTests {

    @Test
    fun `it should parse an event of type ENTITY_CREATE`() {
        val parsedEvent = parseEntitiesEvent(
            ClassPathResource("/ngsild/events/entityCreateEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is EntityCreateEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_UPDATE`() {
        val parsedEvent = parseEntitiesEvent(
            ClassPathResource("/ngsild/events/entityUpdateEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is EntityUpdateEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_DELETE`() {
        val parsedEvent = parseEntitiesEvent(
            ClassPathResource("/ngsild/events/entityDeleteEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is EntityDeleteEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_REPLACE`() {
        val parsedEvent = parseEntitiesEvent(
            ClassPathResource("/ngsild/events/attributeReplaceEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is AttributeReplaceEvent)
    }
}
