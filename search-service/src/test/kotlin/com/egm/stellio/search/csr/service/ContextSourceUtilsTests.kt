package com.egm.stellio.search.csr.service

import arrow.core.right
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_TERM
import com.fasterxml.jackson.module.kotlin.readValue
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class ContextSourceUtilsTests {
    private val name = "name"
    private val minimalEntity: CompactedEntity = mapper.readValue(loadSampleData("beehive_minimal.jsonld"))
    private val baseEntity: CompactedEntity = mapper.readValue(loadSampleData("beehive.jsonld"))
    private val multipleTypeEntity: CompactedEntity = mapper.readValue(loadSampleData("entity_with_multi_types.jsonld"))
    private val time = "2010-01-01T01:01:01.01Z"
    private val moreRecentTime = "2020-01-01T01:01:01.01Z"
    private val evenMoreRecentTime = "2030-01-01T01:01:01.01Z"
    private val nameAttribute: CompactedAttributeInstance = mapOf(
        JSONLD_TYPE_TERM to "Property",
        JSONLD_VALUE_TERM to "name",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to time
    )

    private val moreRecentAttribute: CompactedAttributeInstance = mapOf(
        JSONLD_TYPE_TERM to "Property",
        JSONLD_VALUE_TERM to "moreRecentName",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to moreRecentTime
    )

    private val evenMoreRecentAttribute: CompactedAttributeInstance = mapOf(
        JSONLD_TYPE_TERM to "Property",
        JSONLD_VALUE_TERM to "evenMoreRecentName",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to evenMoreRecentTime
    )

    private val moreRecentEntity = minimalEntity.toMutableMap() + (name to moreRecentAttribute)
    private val evenMoreRecentEntity = minimalEntity.toMutableMap() + (name to evenMoreRecentAttribute)

    private val entityWithName = minimalEntity.toMutableMap().plus(name to nameAttribute)
    private val entityWithLastName = minimalEntity.toMutableMap().plus("lastName" to nameAttribute)
    private val entityWithSurName = minimalEntity.toMutableMap().plus("surName" to nameAttribute)

    @Test
    fun `merge entity should return localEntity when no other entities is provided`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(baseEntity, emptyList())
        assertEquals(baseEntity, mergedEntity.getOrNull())
    }

    @Test
    fun `merge entity should merge the localEntity with the list of entities `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(minimalEntity, listOf(baseEntity to Mode.AUXILIARY))
        assertEquals(baseEntity, mergedEntity.getOrNull())
    }

    @Test
    fun `merge entity should merge all the entities`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithName,
            listOf(entityWithLastName to Mode.AUXILIARY, entityWithSurName to Mode.INCLUSIVE)
        )
        assertEquals(entityWithName + entityWithLastName + entityWithSurName, mergedEntity.getOrNull())
    }

    @Test
    fun `merge entity should call mergeAttribute or mergeTypeOrScope when keys are equals`() = runTest {
        mockkObject(ContextSourceUtils) {
            every { ContextSourceUtils.mergeAttribute(any(), any(), any()) } returns listOf(
                nameAttribute
            ).right()
            every { ContextSourceUtils.mergeTypeOrScope(any(), any()) } returns listOf("Beehive")
            ContextSourceUtils.mergeEntities(
                entityWithName,
                listOf(entityWithName to Mode.AUXILIARY, entityWithName to Mode.INCLUSIVE)
            )
            verify(exactly = 2) { ContextSourceUtils.mergeAttribute(any(), any(), any()) }
            verify(exactly = 2) { ContextSourceUtils.mergeTypeOrScope(any(), any()) }
        }
    }

    @Test
    fun `merge entity should merge the types correctly `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(
            minimalEntity,
            listOf(multipleTypeEntity to Mode.AUXILIARY, baseEntity to Mode.INCLUSIVE)
        ).getOrNull()
        assertThat(mergedEntity?.get(JSONLD_TYPE_TERM) as List<*>)
            .hasSize(3)
            .contains("Sensor", "BeeHive", "Beekeeper")
    }

    @Test
    fun `merge entity should keep both attribute if they have different datasetId `() = runTest {
        val nameAttribute2: CompactedAttributeInstance = mapOf(
            JSONLD_TYPE_TERM to "Property",
            JSONLD_VALUE_TERM to "name2",
            NGSILD_DATASET_ID_TERM to "2"
        )
        val entityWithDifferentName = minimalEntity.toMutableMap() + (name to nameAttribute2)
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithName,
            listOf(entityWithDifferentName to Mode.AUXILIARY, baseEntity to Mode.INCLUSIVE)
        ).getOrNull()
        assertThat(
            mergedEntity?.get(name) as List<*>
        ).hasSize(3).contains(nameAttribute, nameAttribute2, baseEntity[name])
    }

    @Test
    fun `merge entity should merge attribute same datasetId keeping the most recentOne `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithName,
            listOf(evenMoreRecentEntity to Mode.EXCLUSIVE, moreRecentEntity to Mode.INCLUSIVE)
        ).getOrNull()
        assertEquals(
            evenMoreRecentTime,
            (mergedEntity?.get(name) as CompactedAttributeInstance)[NGSILD_OBSERVED_AT_TERM]
        )
        assertEquals("evenMoreRecentName", (mergedEntity[name] as CompactedAttributeInstance)[JSONLD_VALUE_TERM])
    }

    @Test
    fun `merge entity should not merge Auxiliary entity `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithName,
            listOf(evenMoreRecentEntity to Mode.AUXILIARY, moreRecentEntity to Mode.AUXILIARY)
        ).getOrNull()
        assertEquals(
            time,
            (mergedEntity?.get(name) as CompactedAttributeInstance)[NGSILD_OBSERVED_AT_TERM]
        )
        assertEquals("name", (mergedEntity[name] as CompactedAttributeInstance)[JSONLD_VALUE_TERM])
    }

    @Test
    fun `merge entity should keep more recent modifiedAt`() = runTest {
        val entity = minimalEntity.toMutableMap() + (NGSILD_MODIFIED_AT_TERM to time)
        val recentlyModifiedEntity = minimalEntity.toMutableMap() + (NGSILD_MODIFIED_AT_TERM to moreRecentTime)
        val evenMoreRecentlyModifiedEntity =
            minimalEntity.toMutableMap() + (NGSILD_MODIFIED_AT_TERM to evenMoreRecentTime)

        val mergedEntity = ContextSourceUtils.mergeEntities(
            entity,
            listOf(evenMoreRecentlyModifiedEntity to Mode.AUXILIARY, recentlyModifiedEntity to Mode.AUXILIARY)
        )
        assertTrue(mergedEntity.isRight())
        assertEquals(
            evenMoreRecentTime,
            (mergedEntity.getOrNull()?.get(NGSILD_MODIFIED_AT_TERM))
        )
    }

    @Test
    fun `merge entity should keep least recent createdAt`() = runTest {
        val entity = minimalEntity.toMutableMap() + (NGSILD_CREATED_AT_TERM to time)
        val recentlyModifiedEntity = minimalEntity.toMutableMap() + (NGSILD_CREATED_AT_TERM to moreRecentTime)
        val evenMoreRecentlyModifiedEntity =
            minimalEntity.toMutableMap() + (NGSILD_CREATED_AT_TERM to evenMoreRecentTime)

        val mergedEntity = ContextSourceUtils.mergeEntities(
            entity,
            listOf(evenMoreRecentlyModifiedEntity to Mode.AUXILIARY, recentlyModifiedEntity to Mode.AUXILIARY)
        )
        assertTrue(mergedEntity.isRight())

        assertEquals(
            time,
            (mergedEntity.getOrNull()?.get(NGSILD_CREATED_AT_TERM))
        )
    }
}
