package com.egm.stellio.search.model

import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.shared.util.*
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset

class EntityModelTests {

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private val entityPayload = EntityPayload(
        entityId = "urn:ngsi-ld:beehive:01".toUri(),
        types = listOf(BEEHIVE_TYPE),
        createdAt = now,
        modifiedAt = now,
        payload = EMPTY_JSON_PAYLOAD,
        contexts = DEFAULT_CONTEXTS
    )

    @Test
    fun `it should serialize entityPayload with createdAt and modifiedAt`() {
        val serializedEntity = entityPayload.serializeProperties()
        assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
        assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }

    @Test
    fun `it should serialize entityPayload with SAP if present`() {
        val entityPayloadWithSAP =
            entityPayload.copy(specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.AUTH_WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties()
        assertTrue(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }
}
