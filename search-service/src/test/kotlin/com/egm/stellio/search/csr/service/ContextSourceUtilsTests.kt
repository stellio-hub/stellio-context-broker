package com.egm.stellio.search.csr.service

import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_TERM
import com.fasterxml.jackson.module.kotlin.readValue
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class ContextSourceUtilsTests {
    private val name = "name"
    private val minimalEntity: CompactedEntity = mapper.readValue(loadSampleData("beehive_minimal.jsonld"))
    private val baseEntity: CompactedEntity = mapper.readValue(loadSampleData("beehive.jsonld"))
    private val multipleTypeEntity: CompactedEntity = mapper.readValue(loadSampleData("entity_with_multi_types.jsonld"))

    private val nameAttribute: CompactedAttributeInstance = mapOf(
        JSONLD_TYPE_TERM to "Property",
        JSONLD_VALUE_TERM to "name",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to "2010-01-01T01:01:01.01Z"
    )

    val moreRecentAttribute: CompactedAttributeInstance = mapOf(
        JSONLD_TYPE_TERM to "Property",
        JSONLD_VALUE_TERM to "moreRecentName",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to "2020-01-01T01:01:01.01Z"
    )

    val evenMoreRecentAttribute: CompactedAttributeInstance = mapOf(
        JSONLD_TYPE_TERM to "Property",
        JSONLD_VALUE_TERM to "evenMoreRecentName",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to "2030-01-01T01:01:01.01Z"
    )

    val moreRecentEntity = minimalEntity.toMutableMap() + (name to moreRecentAttribute)
    val evenMoreRecentEntity = minimalEntity.toMutableMap() + (name to evenMoreRecentAttribute)

    private val entityWithName = minimalEntity.toMutableMap().plus(name to nameAttribute)
    private val entityWithLastName = minimalEntity.toMutableMap().plus("lastName" to nameAttribute)
    private val entityWithSurName = minimalEntity.toMutableMap().plus("surName" to nameAttribute)

    @Test
    fun `merge entity should return localEntity when no other entities is provided`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntity(baseEntity, emptyList())
        assertEquals(baseEntity, mergedEntity)
    }

    @Test
    fun `merge entity should merge the localEntity with the list of entities `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntity(minimalEntity, listOf(baseEntity to Mode.AUXILIARY))
        assertEquals(baseEntity, mergedEntity)
    }

    @Test
    fun `merge entity should merge all the entities`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntity(
            entityWithName,
            listOf(entityWithLastName to Mode.AUXILIARY, entityWithSurName to Mode.INCLUSIVE)
        )
        assertEquals(entityWithName + entityWithLastName + entityWithSurName, mergedEntity)
    }

    @Test
    fun `merge entity should call mergeAttribute or mergeTypeOrScope when keys are equals`() = runTest {
        mockkObject(ContextSourceUtils) {
            every { ContextSourceUtils.mergeAttribute(any(), any(), any()) } returns listOf(
                nameAttribute
            )
            every { ContextSourceUtils.mergeTypeOrScope(any(), any()) } returns listOf("Beehive")
            ContextSourceUtils.mergeEntity(
                entityWithName,
                listOf(entityWithName to Mode.AUXILIARY, entityWithName to Mode.INCLUSIVE)
            )
            verify(exactly = 2) { ContextSourceUtils.mergeAttribute(any(), any(), any()) }
            verify(exactly = 2) { ContextSourceUtils.mergeTypeOrScope(any(), any()) }
        }
    }

    @Test
    fun `merge entity should merge the types correctly `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntity(
            minimalEntity,
            listOf(multipleTypeEntity to Mode.AUXILIARY, baseEntity to Mode.INCLUSIVE)
        )!!
        assertThat(mergedEntity[JSONLD_TYPE_TERM] as List<*>)
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
        val mergedEntity = ContextSourceUtils.mergeEntity(
            entityWithName,
            listOf(entityWithDifferentName to Mode.AUXILIARY, baseEntity to Mode.INCLUSIVE)
        )!!
        assertThat(mergedEntity[name] as List<*>).hasSize(3).contains(nameAttribute, nameAttribute2, baseEntity[name])
    }

    @Test
    fun `merge entity should merge attribute same datasetId keeping the most recentOne `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntity(
            entityWithName,
            listOf(evenMoreRecentEntity to Mode.EXCLUSIVE, moreRecentEntity to Mode.INCLUSIVE)
        )!!
        assertEquals(
            "2030-01-01T01:01:01.01Z",
            (mergedEntity[name] as CompactedAttributeInstance)[NGSILD_OBSERVED_AT_TERM]
        )
        assertEquals("evenMoreRecentName", (mergedEntity[name] as CompactedAttributeInstance)[JSONLD_VALUE_TERM])
    }

    @Test
    fun `merge entity should not merge Auxiliary entity `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntity(
            entityWithName,
            listOf(evenMoreRecentEntity to Mode.AUXILIARY, moreRecentEntity to Mode.AUXILIARY)
        )!!
        assertEquals(
            "2010-01-01T01:01:01.01Z",
            (mergedEntity[name] as CompactedAttributeInstance)[NGSILD_OBSERVED_AT_TERM]
        )
        assertEquals("name", (mergedEntity[name] as CompactedAttributeInstance)[JSONLD_VALUE_TERM])
    }
}
