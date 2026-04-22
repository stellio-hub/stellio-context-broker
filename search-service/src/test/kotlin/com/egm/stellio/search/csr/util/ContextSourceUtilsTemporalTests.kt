package com.egm.stellio.search.csr.util

import arrow.core.left
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.shared.util.toUri
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import tools.jackson.module.kotlin.readValue

@ActiveProfiles("test")
class ContextSourceUtilsTemporalTests {

    private val minimalEntity: CompactedEntity = mapper.readValue(loadSampleData("beehive_minimal.jsonld"))

    private val time = "2010-01-01T01:01:01.01Z"
    private val moreRecentTime = "2020-01-01T01:01:01.01Z"
    private val evenMoreRecentTime = "2030-01-01T01:01:01.01Z"

    private val temperatureInstance1: CompactedAttributeInstance = mapOf(
        NGSILD_TYPE_TERM to "Property",
        NGSILD_VALUE_TERM to 22.5,
        "instanceId" to "urn:ngsi-ld:Instance:instance1",
        "observedAt" to time
    )

    private val temperatureInstance2: CompactedAttributeInstance = mapOf(
        NGSILD_TYPE_TERM to "Property",
        NGSILD_VALUE_TERM to 24.0,
        "instanceId" to "urn:ngsi-ld:Instance:instance2",
        "observedAt" to moreRecentTime
    )

    private val temperatureInstance3: CompactedAttributeInstance = mapOf(
        NGSILD_TYPE_TERM to "Property",
        NGSILD_VALUE_TERM to 26.0,
        "instanceId" to "urn:ngsi-ld:Instance:instance3",
        "observedAt" to evenMoreRecentTime
    )

    private val entityWithTemperatureList: CompactedEntity =
        minimalEntity.toMutableMap() + ("temperature" to listOf(temperatureInstance1, temperatureInstance2))

    private val entityWithTemperatureInstance3: CompactedEntity =
        minimalEntity.toMutableMap() + ("temperature" to temperatureInstance3)

    private val inclusiveCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri())
    private val auxiliaryCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri(), mode = Mode.AUXILIARY)

    // ------------------- mergeTemporalAttribute -------------------

    @Test
    fun `mergeTemporalAttribute should concatenate all instances from two list attributes`() {
        val result = ContextSourceUtils.mergeTemporalAttribute(
            listOf(temperatureInstance1),
            listOf(temperatureInstance2, temperatureInstance3),
            inclusiveCSR
        )

        Assertions.assertTrue(result.isRight())
        @Suppress("UNCHECKED_CAST")
        val instances = result.getOrNull() as List<CompactedAttributeInstance>
        org.assertj.core.api.Assertions.assertThat(instances)
            .hasSize(3).contains(temperatureInstance1, temperatureInstance2, temperatureInstance3)
    }

    @Test
    fun `mergeTemporalAttribute should normalise a single map instance to a list before concatenation`() {
        val result = ContextSourceUtils.mergeTemporalAttribute(
            temperatureInstance1,
            listOf(temperatureInstance2),
            inclusiveCSR
        )

        Assertions.assertTrue(result.isRight())
        @Suppress("UNCHECKED_CAST")
        val instances = result.getOrNull() as List<CompactedAttributeInstance>
        org.assertj.core.api.Assertions.assertThat(instances)
            .hasSize(2).contains(temperatureInstance1, temperatureInstance2)
    }

    @Test
    fun `mergeTemporalAttribute should normalise two single map instances to lists`() {
        val result = ContextSourceUtils.mergeTemporalAttribute(
            temperatureInstance1,
            temperatureInstance2,
            inclusiveCSR
        )

        Assertions.assertTrue(result.isRight())
        @Suppress("UNCHECKED_CAST")
        val instances = result.getOrNull() as List<CompactedAttributeInstance>
        org.assertj.core.api.Assertions.assertThat(instances)
            .hasSize(2).contains(temperatureInstance1, temperatureInstance2)
    }

    @Test
    fun `mergeTemporalAttribute should leave local list unchanged when remote list is empty`() {
        val result = ContextSourceUtils.mergeTemporalAttribute(
            listOf(temperatureInstance1, temperatureInstance2),
            emptyList<CompactedAttributeInstance>(),
            inclusiveCSR
        )

        Assertions.assertTrue(result.isRight())
        @Suppress("UNCHECKED_CAST")
        val instances = result.getOrNull() as List<CompactedAttributeInstance>
        org.assertj.core.api.Assertions.assertThat(instances)
            .hasSize(2).contains(temperatureInstance1, temperatureInstance2)
    }

    @Test
    fun `mergeTemporalAttribute should return a RevalidationFailedWarning for malformed current input`() {
        val result = ContextSourceUtils.mergeTemporalAttribute(
            "invalidAttribute",
            listOf(temperatureInstance1),
            inclusiveCSR
        )

        Assertions.assertTrue(result.isLeft())
        Assertions.assertInstanceOf(RevalidationFailedWarning::class.java, result.leftOrNull())
    }

    @Test
    fun `mergeTemporalAttribute should return a RevalidationFailedWarning for malformed remote input`() {
        val result = ContextSourceUtils.mergeTemporalAttribute(
            listOf(temperatureInstance1),
            "invalidAttribute",
            inclusiveCSR
        )

        Assertions.assertTrue(result.isLeft())
        Assertions.assertInstanceOf(RevalidationFailedWarning::class.java, result.leftOrNull())
    }

    // ------------------- mergeTemporalEntities -------------------

    @Test
    fun `mergeTemporalEntities should return null when both local and remote are empty`() = runTest {
        val result = ContextSourceUtils.mergeTemporalEntities(null, emptyList())
        Assertions.assertTrue(result.isRight())
        Assertions.assertEquals(null, result.getOrNull())
    }

    @Test
    fun `mergeTemporalEntities should return localEntity when no remote entities provided`() = runTest {
        val result = ContextSourceUtils.mergeTemporalEntities(entityWithTemperatureList, emptyList())
        Assertions.assertEquals(entityWithTemperatureList, result.getOrNull())
    }

    @Test
    fun `mergeTemporalEntities should return remote entity when local is null`() = runTest {
        val result = ContextSourceUtils.mergeTemporalEntities(null, listOf(entityWithTemperatureList to inclusiveCSR))
        Assertions.assertEquals(entityWithTemperatureList, result.getOrNull())
    }

    @Test
    fun `mergeTemporalEntities should concatenate all instances from local and remote`() = runTest {
        val result = ContextSourceUtils.mergeTemporalEntities(
            entityWithTemperatureList,
            listOf(entityWithTemperatureInstance3 to inclusiveCSR)
        ).getOrNull()

        @Suppress("UNCHECKED_CAST")
        val instances = result?.get("temperature") as List<CompactedAttributeInstance>
        org.assertj.core.api.Assertions.assertThat(instances).hasSize(3)
            .contains(temperatureInstance1, temperatureInstance2, temperatureInstance3)
    }

    @Test
    fun `mergeTemporalEntities should concatenate instances from multiple remote CSRs`() = runTest {
        val entityWithInstance2: CompactedEntity =
            minimalEntity.toMutableMap() + ("temperature" to temperatureInstance2)
        val entityWithInstance3: CompactedEntity =
            minimalEntity.toMutableMap() + ("temperature" to temperatureInstance3)

        val result = ContextSourceUtils.mergeTemporalEntities(
            minimalEntity.toMutableMap() + ("temperature" to temperatureInstance1),
            listOf(entityWithInstance2 to inclusiveCSR, entityWithInstance3 to auxiliaryCSR)
        ).getOrNull()

        @Suppress("UNCHECKED_CAST")
        val instances = result?.get("temperature") as List<CompactedAttributeInstance>
        org.assertj.core.api.Assertions.assertThat(instances).hasSize(3)
            .contains(temperatureInstance1, temperatureInstance2, temperatureInstance3)
    }

    @Test
    fun `mergeTemporalEntities should keep least recent createdAt`() = runTest {
        val localEntity = minimalEntity.toMutableMap() + (NGSILD_CREATED_AT_TERM to moreRecentTime)
        val remoteEntity = minimalEntity.toMutableMap() + (NGSILD_CREATED_AT_TERM to time)

        val result = ContextSourceUtils.mergeTemporalEntities(localEntity, listOf(remoteEntity to inclusiveCSR))
        Assertions.assertEquals(time, result.getOrNull()?.get(NGSILD_CREATED_AT_TERM))
    }

    @Test
    fun `mergeTemporalEntities should keep most recent modifiedAt`() = runTest {
        val localEntity = minimalEntity.toMutableMap() + (NGSILD_MODIFIED_AT_TERM to time)
        val remoteEntity = minimalEntity.toMutableMap() + (NGSILD_MODIFIED_AT_TERM to moreRecentTime)

        val result = ContextSourceUtils.mergeTemporalEntities(localEntity, listOf(remoteEntity to inclusiveCSR))
        Assertions.assertEquals(moreRecentTime, result.getOrNull()?.get(NGSILD_MODIFIED_AT_TERM))
    }

    @Test
    fun `mergeTemporalEntities should return warnings from attribute merge failures`() = runTest {
        val warning = RevalidationFailedWarning("bad payload", inclusiveCSR)
        mockkObject(ContextSourceUtils) {
            every { ContextSourceUtils.mergeTemporalAttribute(any(), any(), any()) } returns warning.left()

            val entityWithInvalidAttr: CompactedEntity =
                minimalEntity.toMutableMap() + ("temperature" to "invalidAttribute")
            val localEntity: CompactedEntity =
                minimalEntity.toMutableMap() + ("temperature" to temperatureInstance1)

            val (warnings, entity) = ContextSourceUtils.mergeTemporalEntities(
                localEntity,
                listOf(entityWithInvalidAttr to inclusiveCSR)
            ).toPair()

            org.assertj.core.api.Assertions.assertThat(warnings).hasSize(1)
            Assertions.assertEquals(localEntity, entity)
        }
    }

    // ------------------- mergeTemporalEntitiesLists -------------------

    @Test
    fun `mergeTemporalEntitiesLists should merge entities with the same id`() = runTest {
        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(entityWithTemperatureList),
            listOf(listOf(entityWithTemperatureInstance3) to inclusiveCSR)
        ).getOrNull()

        Assertions.assertEquals(1, result!!.size)
        @Suppress("UNCHECKED_CAST")
        val instances = result.first()["temperature"] as List<CompactedAttributeInstance>
        org.assertj.core.api.Assertions.assertThat(instances).hasSize(3)
    }

    @Test
    fun `mergeTemporalEntitiesLists should add entities with different ids`() = runTest {
        val entityWithDifferentId = minimalEntity.toMutableMap() + (NGSILD_ID_TERM to "differentId")

        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(entityWithTemperatureList),
            listOf(listOf(entityWithDifferentId) to inclusiveCSR)
        ).getOrNull()

        org.assertj.core.api.Assertions.assertThat(result).hasSize(2)
    }

    @Test
    fun `mergeTemporalEntitiesLists should include remote-only entities not present locally`() = runTest {
        val remoteOnlyEntity = (minimalEntity.toMutableMap() + (NGSILD_ID_TERM to "urn:ngsi-ld:Entity:remote"))
            .plus("temperature" to temperatureInstance1)

        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            emptyList(),
            listOf(listOf(remoteOnlyEntity) to inclusiveCSR)
        ).getOrNull()

        org.assertj.core.api.Assertions.assertThat(result).hasSize(1).contains(remoteOnlyEntity)
    }

    @Test
    fun `mergeTemporalEntitiesLists should not deduplicate instances from auxiliary CSRs`() = runTest {
        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(entityWithTemperatureList),
            listOf(listOf(entityWithTemperatureInstance3) to auxiliaryCSR)
        ).getOrNull()

        Assertions.assertEquals(1, result!!.size)
        @Suppress("UNCHECKED_CAST")
        val instances = result.first()["temperature"] as List<CompactedAttributeInstance>
        org.assertj.core.api.Assertions.assertThat(instances).hasSize(3)
            .contains(temperatureInstance1, temperatureInstance2, temperatureInstance3)
    }

    @Test
    fun `mergeTemporalEntitiesLists should return warnings when attribute merge fails`() = runTest {
        val warning = RevalidationFailedWarning("bad payload", inclusiveCSR)
        mockkObject(ContextSourceUtils) {
            every { ContextSourceUtils.getMergeTemporalNewValues(any(), any(), any()) } returns warning.left()

            val (warnings, entities) = ContextSourceUtils.mergeTemporalEntitiesLists(
                listOf(entityWithTemperatureList),
                listOf(listOf(entityWithTemperatureList) to inclusiveCSR)
            ).toPair()

            verify(exactly = 1) { ContextSourceUtils.getMergeTemporalNewValues(any(), any(), any()) }
            org.assertj.core.api.Assertions.assertThat(warnings).hasSize(1)
            Assertions.assertEquals(listOf(entityWithTemperatureList), entities)
        }
    }
}
