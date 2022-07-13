package com.egm.stellio.entity.model

import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVATION_SPACE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OPERATION_SPACE_PROPERTY
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset

class EntityTest {

    private val entity = Entity(
        id = "urn:ngsi-ld:beehive:01".toUri(),
        types = listOf("Beehive"),
        createdAt = Instant.now().atZone(ZoneOffset.UTC),
        modifiedAt = Instant.now().atZone(ZoneOffset.UTC)
    )

    @Test
    fun `it should serialize entity without createdAt and modifiedAt if not specified`() {
        val serializedEntity = entity.serializeCoreProperties(false)
        Assertions.assertFalse(serializedEntity.contains(NGSILD_CREATED_AT_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(NGSILD_MODIFIED_AT_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(NGSILD_LOCATION_PROPERTY))
        Assertions.assertEquals(setOf(JSONLD_ID, JSONLD_TYPE), serializedEntity.keys)
    }

    @Test
    fun `it should serialize entity with createdAt and modifiedAt if specified`() {
        val serializedEntity = entity.serializeCoreProperties(true)
        Assertions.assertTrue(serializedEntity.contains(NGSILD_CREATED_AT_PROPERTY))
        Assertions.assertTrue(serializedEntity.contains(NGSILD_MODIFIED_AT_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(NGSILD_LOCATION_PROPERTY))
    }

    @Test
    fun `it should serialize entity with location if specified`() {
        val entityWithLocation = entity.copy(location = "POINT (24.30623 60.07966)")
        val serializedEntity = entityWithLocation.serializeCoreProperties(true)
        Assertions.assertTrue(serializedEntity.contains(NGSILD_LOCATION_PROPERTY))
        Assertions.assertEquals(
            mapOf(JSONLD_TYPE to "GeoProperty", JsonLdUtils.NGSILD_GEOPROPERTY_VALUE to "POINT (24.30623 60.07966)"),
            serializedEntity[NGSILD_LOCATION_PROPERTY]
        )
        Assertions.assertFalse(serializedEntity.contains(NGSILD_OPERATION_SPACE_PROPERTY))
        Assertions.assertFalse(serializedEntity.contains(NGSILD_OBSERVATION_SPACE_PROPERTY))
    }

    @Test
    fun `it should serialize entity with all geo properties if specified`() {
        val entityWithLocation = entity.copy(
            location = "POINT (24.30623 60.07966)",
            operationSpace = "POINT (25.30623 62.07966)",
            observationSpace = "POINT (26.30623 58.07966)"
        )
        val serializedEntity = entityWithLocation.serializeCoreProperties(true)
        Assertions.assertTrue(serializedEntity.contains(NGSILD_LOCATION_PROPERTY))
        Assertions.assertEquals(
            mapOf(JSONLD_TYPE to "GeoProperty", JsonLdUtils.NGSILD_GEOPROPERTY_VALUE to "POINT (24.30623 60.07966)"),
            serializedEntity[NGSILD_LOCATION_PROPERTY]
        )
        Assertions.assertTrue(serializedEntity.contains(NGSILD_OPERATION_SPACE_PROPERTY))
        Assertions.assertEquals(
            mapOf(JSONLD_TYPE to "GeoProperty", JsonLdUtils.NGSILD_GEOPROPERTY_VALUE to "POINT (25.30623 62.07966)"),
            serializedEntity[NGSILD_OPERATION_SPACE_PROPERTY]
        )
        Assertions.assertTrue(serializedEntity.contains(NGSILD_OBSERVATION_SPACE_PROPERTY))
        Assertions.assertEquals(
            mapOf(JSONLD_TYPE to "GeoProperty", JsonLdUtils.NGSILD_GEOPROPERTY_VALUE to "POINT (26.30623 58.07966)"),
            serializedEntity[NGSILD_OBSERVATION_SPACE_PROPERTY]
        )
    }
}
