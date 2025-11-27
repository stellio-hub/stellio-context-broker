package com.egm.stellio.search.entity.model

import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_IRI
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.UriUtils.toUri
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset

class EntityModelTests {

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private val entity = Entity(
        entityId = "urn:ngsi-ld:beehive:01".toUri(),
        types = listOf(BEEHIVE_IRI),
        createdAt = now,
        modifiedAt = now,
        payload = EMPTY_JSON_PAYLOAD
    )

    @Test
    fun `it should serialize entityPayload with createdAt and modifiedAt`() {
        val serializedEntity = entity.serializeProperties()
        assertTrue(serializedEntity.contains(NGSILD_CREATED_AT_IRI))
        assertTrue(serializedEntity.contains(NGSILD_MODIFIED_AT_IRI))
        assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }

    @Test
    fun `it should serialize entityPayload with SAP if present`() {
        val entityPayloadWithSAP =
            entity.copy(specificAccessPolicy = Action.WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties()
        assertTrue(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }
}
