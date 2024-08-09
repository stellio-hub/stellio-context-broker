package com.egm.stellio.search.entity.model

import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset

class EntityModelTests {

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private val entity = Entity(
        entityId = "urn:ngsi-ld:beehive:01".toUri(),
        types = listOf(BEEHIVE_TYPE),
        createdAt = now,
        modifiedAt = now,
        payload = EMPTY_JSON_PAYLOAD
    )

    @Test
    fun `it should serialize entityPayload with createdAt and modifiedAt`() {
        val serializedEntity = entity.serializeProperties()
        assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
        assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }

    @Test
    fun `it should serialize entityPayload with SAP if present`() {
        val entityPayloadWithSAP =
            entity.copy(specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.AUTH_WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties()
        assertTrue(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }
}
