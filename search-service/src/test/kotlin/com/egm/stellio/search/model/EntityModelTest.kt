package com.egm.stellio.search.model

import com.egm.stellio.shared.util.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset

class EntityModelTest {

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private val entityPayload = EntityPayload(
        entityId = "urn:ngsi-ld:beehive:01".toUri(),
        types = listOf(BEEHIVE_TYPE),
        createdAt = now,
        modifiedAt = now,
        contexts = DEFAULT_CONTEXTS
    )

    @Test
    fun `it should serialize entityPayload without createdAt and modifiedAt if not specified`() {
        val serializedEntity = entityPayload.serializeProperties(false)
        Assertions.assertFalse(serializedEntity.contains(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
        Assertions.assertEquals(setOf(JsonLdUtils.JSONLD_ID, JsonLdUtils.JSONLD_TYPE), serializedEntity.keys)
        Assertions.assertEquals("urn:ngsi-ld:beehive:01", serializedEntity[JsonLdUtils.JSONLD_ID])
        Assertions.assertEquals(listOf(BEEHIVE_TYPE), serializedEntity[JsonLdUtils.JSONLD_TYPE])
    }

    @Test
    fun `it should serialize entityPayload with createdAt and modifiedAt if specified`() {
        val serializedEntity = entityPayload.serializeProperties(true)
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
        val createdAt = mapOf(
            JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
            JsonLdUtils.JSONLD_VALUE_KW to now.toNgsiLdFormat()
        )
        Assertions.assertEquals(createdAt, serializedEntity[JsonLdUtils.NGSILD_CREATED_AT_PROPERTY])
        val modifiedAt = mapOf(
            JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
            JsonLdUtils.JSONLD_VALUE_KW to now.toNgsiLdFormat()
        )
        Assertions.assertEquals(modifiedAt, serializedEntity[JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY])
        Assertions.assertFalse(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
    }

    @Test
    fun `it should serialize entityPayload with SAP if present`() {
        val entityPayloadWithSAP =
            entityPayload.copy(specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.AUTH_WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties(false)
        val specificAccessPolicy = mapOf(
            JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_PROPERTY_TYPE,
            JsonLdUtils.JSONLD_VALUE_KW to AuthContextModel.SpecificAccessPolicy.AUTH_WRITE
        )
        Assertions.assertTrue(serializedEntity.contains(AuthContextModel.AUTH_PROP_SAP))
        Assertions.assertEquals(specificAccessPolicy, serializedEntity[AuthContextModel.AUTH_PROP_SAP])

        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_ID))
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_TYPE))
        Assertions.assertEquals("urn:ngsi-ld:beehive:01", serializedEntity[JsonLdUtils.JSONLD_ID])
        Assertions.assertEquals(listOf(BEEHIVE_TYPE), serializedEntity[JsonLdUtils.JSONLD_TYPE])
    }

    @Test
    fun `it should serialize entityPayload with SAP if present and compact term if specified`() {
        val entityPayloadWithSAP =
            entityPayload.copy(specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.AUTH_WRITE)
        val serializedEntity = entityPayloadWithSAP.serializeProperties(false, true, listOf(APIC_COMPOUND_CONTEXT))
        val specificAccessPolicy = mapOf(
            JsonLdUtils.JSONLD_TYPE_TERM to "Property",
            JsonLdUtils.JSONLD_VALUE to AuthContextModel.SpecificAccessPolicy.AUTH_WRITE
        )
        Assertions.assertTrue(serializedEntity.contains(AuthContextModel.AUTH_TERM_SAP))
        Assertions.assertEquals(specificAccessPolicy, serializedEntity[AuthContextModel.AUTH_TERM_SAP])

        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_ID_TERM))
        Assertions.assertTrue(serializedEntity.contains(JsonLdUtils.JSONLD_TYPE_TERM))
        Assertions.assertEquals("urn:ngsi-ld:beehive:01", serializedEntity[JsonLdUtils.JSONLD_ID_TERM])
        Assertions.assertEquals(BEEHIVE_COMPACT_TYPE, serializedEntity[JsonLdUtils.JSONLD_TYPE_TERM])
    }
}
