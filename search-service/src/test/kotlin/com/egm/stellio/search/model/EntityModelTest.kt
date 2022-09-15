package com.egm.stellio.search.model

import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset

class EntityModelTest {

    private val entityPayload = EntityPayload(
        entityId = "urn:ngsi-ld:beehive:01".toUri(),
        types = listOf("Beehive"),
        createdAt = Instant.now().atZone(ZoneOffset.UTC),
        modifiedAt = Instant.now().atZone(ZoneOffset.UTC),
        contexts = DEFAULT_CONTEXTS
    )

    @Test
    fun `it should serialize entityPayload without createdAt and modifiedAt if not specified`() {
        val serializedEntity = entityPayload.serializeProperties(false)
        Assertions.assertFalse(serializedEntity.contains(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
        Assertions.assertEquals(setOf(JsonLdUtils.JSONLD_ID, JsonLdUtils.JSONLD_TYPE), serializedEntity.keys)
    }

    @Test
    fun `it should serialize entityPayload with createdAt and modifiedAt if specified`() {
        val serializedEntity = entityPayload.serializeProperties(true)
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }

    @Test
    fun `it should serialize entityPayload with SAP if present`() {
        val entityPayloadWithSAP =
            entityPayload.copy(specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.AUTH_WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties(false)
        Assertions.assertTrue(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_ID))
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_TYPE))
    }

    @Test
    fun `it should serialize entityPayload with SAP if present and compact term if specified`() {
        val entityPayloadWithSAP =
            entityPayload.copy(specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.AUTH_WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties(false, true, DEFAULT_CONTEXTS)
        Assertions.assertTrue(serializedEntity.contains(AuthContextModel.AUTH_TERM_SAP))
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_ID_TERM))
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_TYPE_TERM))
    }
}