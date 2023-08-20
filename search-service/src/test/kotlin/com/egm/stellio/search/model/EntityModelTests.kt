package com.egm.stellio.search.model

import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.shared.util.*
import org.junit.jupiter.api.Assertions.*
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
    fun `it should serialize entityPayload without createdAt and modifiedAt if not specified`() {
        val serializedEntity = entityPayload.serializeProperties(false)
        assertFalse(serializedEntity.contains(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        assertFalse(serializedEntity.contains(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
        assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
        assertEquals(setOf(JsonLdUtils.JSONLD_ID, JsonLdUtils.JSONLD_TYPE), serializedEntity.keys)
        assertEquals("urn:ngsi-ld:beehive:01", serializedEntity[JsonLdUtils.JSONLD_ID])
        assertEquals(listOf(BEEHIVE_TYPE), serializedEntity[JsonLdUtils.JSONLD_TYPE])
    }

    @Test
    fun `it should serialize entityPayload with createdAt and modifiedAt if specified`() {
        val serializedEntity = entityPayload.serializeProperties(true)
        assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
        assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }

    @Test
    fun `it should serialize entityPayload with SAP if present`() {
        val entityPayloadWithSAP =
            entityPayload.copy(specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.AUTH_WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties(false)
        assertTrue(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }

    @Test
    fun `it should serialize entityPayload with SAP if present and compact term if specified`() {
        val entityPayloadWithSAP =
            entityPayload.copy(specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.AUTH_WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties(
            withSysAttrs = false,
            withCompactTerms = true,
            contexts = listOf(APIC_COMPOUND_CONTEXT)
        )
        val specificAccessPolicy = mapOf(
            JsonLdUtils.JSONLD_TYPE_TERM to "Property",
            JsonLdUtils.JSONLD_VALUE_TERM to AuthContextModel.SpecificAccessPolicy.AUTH_WRITE
        )
        assertTrue(serializedEntity.contains(AuthContextModel.AUTH_TERM_SAP))
        assertEquals(specificAccessPolicy, serializedEntity[AuthContextModel.AUTH_TERM_SAP])

        assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_ID_TERM))
        assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_TYPE_TERM))
        assertEquals("urn:ngsi-ld:beehive:01", serializedEntity[JsonLdUtils.JSONLD_ID_TERM])
        assertEquals(BEEHIVE_COMPACT_TYPE, serializedEntity[JsonLdUtils.JSONLD_TYPE_TERM])
    }
}
