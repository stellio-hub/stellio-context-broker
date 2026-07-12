package com.egm.stellio.search.csr.util

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.model.NGSILD_EXPIRES_AT_TERM
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.shared.util.toUri
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import tools.jackson.module.kotlin.readValue

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
        NGSILD_TYPE_TERM to "Property",
        NGSILD_VALUE_TERM to "name",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to time
    )

    private val moreRecentAttribute: CompactedAttributeInstance = mapOf(
        NGSILD_TYPE_TERM to "Property",
        NGSILD_VALUE_TERM to "moreRecentName",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to moreRecentTime
    )

    private val evenMoreRecentAttribute: CompactedAttributeInstance = mapOf(
        NGSILD_TYPE_TERM to "Property",
        NGSILD_VALUE_TERM to "evenMoreRecentName",
        NGSILD_DATASET_ID_TERM to "1",
        NGSILD_OBSERVED_AT_TERM to evenMoreRecentTime
    )

    private val moreRecentEntity = minimalEntity.toMutableMap() + (name to moreRecentAttribute)
    private val evenMoreRecentEntity = minimalEntity.toMutableMap() + (name to evenMoreRecentAttribute)

    private val entityWithName = minimalEntity.toMutableMap().plus(name to nameAttribute)
    private val entityWithLastName = minimalEntity.toMutableMap().plus("lastName" to nameAttribute)
    private val entityWithSurName = minimalEntity.toMutableMap().plus("surName" to nameAttribute)
    private val invalidEntityWithLastName = entityWithLastName.toMutableMap()
        .plus(name to "invalidAttribute")
        .plus(NGSILD_CREATED_AT_TERM to moreRecentTime)

    private val auxiliaryCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri(), mode = Mode.AUXILIARY)
    private val inclusiveCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri())

    @Test
    fun `merge entity should return localEntity when no other entities are provided`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(baseEntity, emptyList())
        assertEquals(baseEntity, mergedEntity.getOrNull())
    }

    @Test
    fun `merge entity should merge the localEntity with the list of entities`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(minimalEntity, listOf(baseEntity to auxiliaryCSR))
        assertEquals(baseEntity, mergedEntity.getOrNull())
    }

    @Test
    fun `merge entity should merge all the entities`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithName,
            listOf(entityWithLastName to auxiliaryCSR, entityWithSurName to inclusiveCSR)
        )
        assertEquals(entityWithName + entityWithLastName + entityWithSurName, mergedEntity.getOrNull())
    }

    @Test
    fun `merge entity should merge the types correctly `() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(
            minimalEntity,
            listOf(multipleTypeEntity to auxiliaryCSR, baseEntity to inclusiveCSR)
        ).getOrNull()
        assertThat(mergedEntity?.get(NGSILD_TYPE_TERM) as List<*>)
            .hasSize(3)
            .contains("Sensor", "BeeHive", "Beekeeper")
    }

    @Test
    fun `merge entity should keep both attribute instances if they have different datasetId `() = runTest {
        val nameAttribute2: CompactedAttributeInstance = mapOf(
            NGSILD_TYPE_TERM to "Property",
            NGSILD_VALUE_TERM to "name2",
            NGSILD_DATASET_ID_TERM to "2"
        )
        val entityWithDifferentName = minimalEntity.toMutableMap() + (name to nameAttribute2)
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithName,
            listOf(entityWithDifferentName to auxiliaryCSR, baseEntity to inclusiveCSR)
        ).getOrNull()
        assertThat(mergedEntity?.get(name) as List<*>)
            .hasSize(3)
            .contains(nameAttribute, nameAttribute2, baseEntity[name])
    }

    @Test
    fun `merge entity should merge attribute with same datasetId keeping the most recent one`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithName,
            listOf(evenMoreRecentEntity to inclusiveCSR, moreRecentEntity to inclusiveCSR)
        ).getOrNull()
        assertEquals(
            evenMoreRecentTime,
            (mergedEntity?.get(name) as CompactedAttributeInstance)[NGSILD_OBSERVED_AT_TERM]
        )
        assertEquals("evenMoreRecentName", (mergedEntity[name] as CompactedAttributeInstance)[NGSILD_VALUE_TERM])
    }

    @Test
    fun `merge attribute should consider a null value before a dated one`() = runTest {
        val attributeWithoutObservedAt = nameAttribute - NGSILD_OBSERVED_AT_TERM

        val mergedWithDatedValueFirst = ContextSourceUtils.mergeAttribute(
            nameAttribute,
            attributeWithoutObservedAt,
            inclusiveCSR
        ).getOrNull()
        val mergedWithNullValueFirst = ContextSourceUtils.mergeAttribute(
            attributeWithoutObservedAt,
            nameAttribute,
            inclusiveCSR
        ).getOrNull()

        assertEquals(nameAttribute, mergedWithDatedValueFirst)
        assertEquals(nameAttribute, mergedWithNullValueFirst)
    }

    @Test
    fun `merge attribute should keep most recent modifiedAt when both observedAt values are null`() = runTest {
        val olderAttribute = nameAttribute - NGSILD_OBSERVED_AT_TERM +
            (NGSILD_MODIFIED_AT_TERM to time)
        val newerAttribute = moreRecentAttribute - NGSILD_OBSERVED_AT_TERM +
            (NGSILD_MODIFIED_AT_TERM to moreRecentTime)

        val mergedWithOlderValueFirst = ContextSourceUtils.mergeAttribute(
            olderAttribute,
            newerAttribute,
            inclusiveCSR
        ).getOrNull()
        val mergedWithNewerValueFirst = ContextSourceUtils.mergeAttribute(
            newerAttribute,
            olderAttribute,
            inclusiveCSR
        ).getOrNull()

        assertEquals(newerAttribute, mergedWithOlderValueFirst)
        assertEquals(newerAttribute, mergedWithNewerValueFirst)
    }

    @Test
    fun `merge entity should not merge info from auxiliary entity if already present`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithName,
            listOf(evenMoreRecentEntity to auxiliaryCSR, moreRecentEntity to auxiliaryCSR)
        ).getOrNull()
        assertEquals(
            time,
            (mergedEntity?.get(name) as CompactedAttributeInstance)[NGSILD_OBSERVED_AT_TERM]
        )
        assertEquals("name", (mergedEntity[name] as CompactedAttributeInstance)[NGSILD_VALUE_TERM])
    }

    @Test
    fun `merge entity should keep most recent modifiedAt`() = runTest {
        val entity = minimalEntity.toMutableMap() + (NGSILD_MODIFIED_AT_TERM to time)
        val recentlyModifiedEntity = minimalEntity.toMutableMap() + (NGSILD_MODIFIED_AT_TERM to moreRecentTime)
        val evenMoreRecentlyModifiedEntity =
            minimalEntity.toMutableMap() + (NGSILD_MODIFIED_AT_TERM to evenMoreRecentTime)

        val mergedEntity = ContextSourceUtils.mergeEntities(
            entity,
            listOf(evenMoreRecentlyModifiedEntity to auxiliaryCSR, recentlyModifiedEntity to auxiliaryCSR)
        )
        assertTrue(mergedEntity.isRight())
        assertEquals(
            evenMoreRecentTime,
            mergedEntity.getOrNull()?.get(NGSILD_MODIFIED_AT_TERM)
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
            listOf(evenMoreRecentlyModifiedEntity to auxiliaryCSR, recentlyModifiedEntity to auxiliaryCSR)
        )
        assertTrue(mergedEntity.isRight())

        assertEquals(
            time,
            mergedEntity.getOrNull()?.get(NGSILD_CREATED_AT_TERM)
        )
    }

    @Test
    fun `merge entity should merge each entity using getMergeNewValues and return the received warnings`() = runTest {
        val warning1 = MiscellaneousWarning("1", inclusiveCSR)
        val warning2 = MiscellaneousWarning("2", inclusiveCSR)
        mockkObject(ContextSourceUtils) {
            every { ContextSourceUtils.getMergeNewValues(any(), any(), any()) } returns
                warning1.left() andThen warning2.left()

            val (warnings, entity) = ContextSourceUtils.mergeEntities(
                entityWithName,
                listOf(entityWithName to inclusiveCSR, entityWithName to inclusiveCSR)
            ).toPair()
            verify(exactly = 2) { ContextSourceUtils.getMergeNewValues(any(), any(), any()) }
            assertThat(warnings).hasSize(2).contains(warning1, warning2)
            assertEquals(entityWithName, entity)
        }
    }

    @Test
    fun `merge entity should call mergeAttribute or mergeTypeOrScope when keys are equal`() = runTest {
        mockkObject(ContextSourceUtils) {
            every { ContextSourceUtils.mergeAttribute(any(), any(), any()) } returns listOf(
                nameAttribute
            ).right()
            every { ContextSourceUtils.mergeTypeOrScope(any(), any()) } returns listOf("Beehive")
            ContextSourceUtils.mergeEntities(
                entityWithName,
                listOf(entityWithName to auxiliaryCSR, entityWithName to inclusiveCSR)
            )
            verify(exactly = 2) { ContextSourceUtils.mergeAttribute(any(), any(), any()) }
            verify(exactly = 2) { ContextSourceUtils.mergeTypeOrScope(any(), any()) }
        }
    }

    @Test
    fun `merge entity should not merge entities in error`() = runTest {
        mockkObject(ContextSourceUtils) {
            val (warnings, entity) = ContextSourceUtils.mergeEntities(
                entityWithName,
                listOf(invalidEntityWithLastName to inclusiveCSR, entityWithSurName to inclusiveCSR)
            ).toPair()

            verify(exactly = 2) { ContextSourceUtils.getMergeNewValues(any(), any(), any()) }
            assertThat(warnings).hasSize(1)
            val entityWithNameAndSurname = entityWithName.plus("surName" to nameAttribute)
            assertEquals(entityWithNameAndSurname, entity)
        }
    }

    @Test
    fun `merge entitiesList should merge entities with the same id`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntitiesLists(
            listOf(entityWithName),
            listOf(listOf(entityWithLastName) to auxiliaryCSR, listOf(entityWithSurName) to inclusiveCSR)
        ).getOrNull()
        assertEquals(1, mergedEntity!!.size)
        assertEquals(entityWithName + entityWithLastName + entityWithSurName, mergedEntity.first())
    }

    @Test
    fun `merge entitiesList should not keep conflicting data from an auxiliary csr`() = runTest {
        val mergedEntity = ContextSourceUtils.mergeEntitiesLists(
            listOf(moreRecentEntity),
            listOf(listOf(evenMoreRecentEntity) to auxiliaryCSR, listOf(entityWithName) to inclusiveCSR)
        ).getOrNull()
        assertEquals(1, mergedEntity!!.size)
        assertEquals(moreRecentEntity, mergedEntity.first())
    }

    @Test
    fun `merge entitiesList should add entities with the different ids`() = runTest {
        val entityWithDifferentId = minimalEntity.toMutableMap() + (NGSILD_ID_TERM to "differentId")
        val entityWithAnotherDifferentId = minimalEntity.toMutableMap() + (NGSILD_ID_TERM to "anotherDifferentId")
        val mergedEntity = ContextSourceUtils.mergeEntitiesLists(
            listOf(entityWithName),
            listOf(listOf(entityWithDifferentId) to inclusiveCSR, listOf(entityWithAnotherDifferentId) to inclusiveCSR)
        ).getOrNull()
        assertThat(mergedEntity)
            .hasSize(3)
            .contains(entityWithName, entityWithDifferentId, entityWithAnotherDifferentId)
    }

    @Test
    fun `mergeEntitiesLists should drop expiresAt if one source is missing it`() = runTest {
        val entityWithExpiresAt = entityWithName.plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val mergedEntity = ContextSourceUtils.mergeEntitiesLists(
            listOf(entityWithExpiresAt),
            listOf(listOf(entityWithLastName) to inclusiveCSR)
        ).getOrNull()

        assertThat(mergedEntity!!.first()).doesNotContainKey(NGSILD_EXPIRES_AT_TERM)
    }

    @Test
    fun `mergeEntitiesLists should keep the furthest in the future expiresAt if all sources have one`() = runTest {
        val entityWithExpiresAt = entityWithName.plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)
        val otherEntityWithExpiresAt = entityWithLastName.plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val mergedEntity = ContextSourceUtils.mergeEntitiesLists(
            listOf(entityWithExpiresAt),
            listOf(listOf(otherEntityWithExpiresAt) to inclusiveCSR)
        ).getOrNull()

        assertEquals(evenMoreRecentTime, mergedEntity!!.first()[NGSILD_EXPIRES_AT_TERM])
    }

    @Test
    fun `merge entitiesList should merge using getMergeNewValues and return the received warnings`() = runTest {
        val warning1 = MiscellaneousWarning("1", inclusiveCSR)
        val warning2 = MiscellaneousWarning("2", inclusiveCSR)
        mockkObject(ContextSourceUtils) {
            every { ContextSourceUtils.getMergeNewValues(any(), any(), any()) } returns
                warning1.left() andThen warning2.left()

            val (warnings, entity) = ContextSourceUtils.mergeEntitiesLists(
                listOf(entityWithName),
                listOf(listOf(entityWithName) to inclusiveCSR, listOf(entityWithName) to inclusiveCSR)
            ).toPair()
            verify(exactly = 2) { ContextSourceUtils.getMergeNewValues(any(), any(), any()) }
            assertThat(warnings).hasSize(2).contains(warning1, warning2)
            assertEquals(listOf(entityWithName), entity)
        }
    }

    @Test
    fun `merge entitiesList should not merge List in error`() = runTest {
        mockkObject(ContextSourceUtils) {
            val (warnings, entity) = ContextSourceUtils.mergeEntitiesLists(
                listOf(entityWithName),
                listOf(
                    listOf(invalidEntityWithLastName) to inclusiveCSR,
                    listOf(entityWithSurName) to inclusiveCSR
                )
            ).toPair()

            verify(exactly = 2) { ContextSourceUtils.getMergeNewValues(any(), any(), any()) }
            assertThat(warnings).hasSize(1)
            val entityWithNameAndSurname = entityWithName.plus("surName" to nameAttribute)
            assertEquals(listOf(entityWithNameAndSurname), entity)
        }
    }

    @Test
    fun `propagateExpiresAtToAttributes should add expiresAt to an attribute missing it`() {
        val entity = entityWithName.plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val entityWithExpiresAtPropagated = ContextSourceUtils.propagateExpiresAtToAttributes(entity)

        assertEquals(
            evenMoreRecentTime,
            (entityWithExpiresAtPropagated[name] as CompactedAttributeInstance)[NGSILD_EXPIRES_AT_TERM]
        )
    }

    @Test
    fun `propagateExpiresAtToAttributes should tighten an attribute expiresAt later than the entity's`() {
        val attributeWithLaterExpiry = nameAttribute.plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val entity = minimalEntity.toMutableMap()
            .plus(name to attributeWithLaterExpiry)
            .plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)
        val entityWithExpiresAtPropagated = ContextSourceUtils.propagateExpiresAtToAttributes(entity)

        assertEquals(
            moreRecentTime,
            (entityWithExpiresAtPropagated[name] as CompactedAttributeInstance)[NGSILD_EXPIRES_AT_TERM]
        )
    }

    @Test
    fun `propagateExpiresAtToAttributes should keep an attribute expiresAt already earlier than the entity's`() {
        val attributeWithEarlierExpiry = nameAttribute.plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)
        val entity = minimalEntity.toMutableMap()
            .plus(name to attributeWithEarlierExpiry)
            .plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val entityWithExpiresAtPropagated = ContextSourceUtils.propagateExpiresAtToAttributes(entity)

        assertEquals(
            moreRecentTime,
            (entityWithExpiresAtPropagated[name] as CompactedAttributeInstance)[NGSILD_EXPIRES_AT_TERM]
        )
    }

    @Test
    fun `propagateExpiresAtToAttributes should leave the entity untouched if it has no expiresAt`() {
        val entityWithExpiresAtPropagated = ContextSourceUtils.propagateExpiresAtToAttributes(entityWithName)

        assertEquals(entityWithName, entityWithExpiresAtPropagated)
    }

    @Test
    fun `propagateExpiresAtToAttributes should propagate to every instance of a multi-instance attribute`() {
        val nameAttribute2: CompactedAttributeInstance = mapOf(
            NGSILD_TYPE_TERM to "Property",
            NGSILD_VALUE_TERM to "name2",
            NGSILD_DATASET_ID_TERM to "2",
            NGSILD_EXPIRES_AT_TERM to time
        )
        val entity = minimalEntity.toMutableMap()
            .plus(name to listOf(nameAttribute, nameAttribute2))
            .plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)

        val entityWithExpiresAtPropagated = ContextSourceUtils.propagateExpiresAtToAttributes(entity)

        val instances = entityWithExpiresAtPropagated[name] as List<*>
        val instanceWithoutOwnExpiry = instances
            .first { (it as CompactedAttributeInstance)[NGSILD_DATASET_ID_TERM] == "1" } as CompactedAttributeInstance
        val instanceWithEarlierOwnExpiry = instances
            .first { (it as CompactedAttributeInstance)[NGSILD_DATASET_ID_TERM] == "2" } as CompactedAttributeInstance

        assertEquals(moreRecentTime, instanceWithoutOwnExpiry[NGSILD_EXPIRES_AT_TERM])
        assertEquals(time, instanceWithEarlierOwnExpiry[NGSILD_EXPIRES_AT_TERM])
    }

    @Test
    fun `mergeEntities should drop expiresAt if one source is missing it`() = runTest {
        val entityWithExpiresAt = entityWithName.plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithExpiresAt,
            listOf(entityWithLastName to inclusiveCSR)
        ).getOrNull()

        assertThat(mergedEntity).doesNotContainKey(NGSILD_EXPIRES_AT_TERM)
    }

    @Test
    fun `mergeEntities should keep the furthest in the future expiresAt if all sources have one`() = runTest {
        val entityWithExpiresAt = entityWithName.plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)
        val otherEntityWithExpiresAt = entityWithLastName.plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val mergedEntity = ContextSourceUtils.mergeEntities(
            entityWithExpiresAt,
            listOf(otherEntityWithExpiresAt to inclusiveCSR)
        ).getOrNull()

        assertEquals(evenMoreRecentTime, mergedEntity?.get(NGSILD_EXPIRES_AT_TERM))
    }

    @Test
    fun `mergeEntities should combine expiration propagation with the entity-level conflict rule`() = runTest {
        // CSR-1: entity-level expiresAt, its 'name' attribute (datasetId 1) has none of its own
        val csr1Entity = entityWithName.plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)

        // CSR-2: no entity-level expiresAt, but its 'name' attribute (datasetId 2) already has one
        val nameAttributeWithOwnExpiry: CompactedAttributeInstance = mapOf(
            NGSILD_TYPE_TERM to "Property",
            NGSILD_VALUE_TERM to "name2",
            NGSILD_DATASET_ID_TERM to "2",
            NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime
        )
        val csr2Entity = minimalEntity.toMutableMap() + (name to nameAttributeWithOwnExpiry)

        val mergedEntity = ContextSourceUtils.mergeEntities(
            minimalEntity,
            listOf(csr1Entity to inclusiveCSR, csr2Entity to inclusiveCSR)
        ).getOrNull()

        // local entity and CSR-2 both lack an entity-level expiresAt -> dropped from the merged entity
        assertThat(mergedEntity).doesNotContainKey(NGSILD_EXPIRES_AT_TERM)

        // CSR-1's attribute had no expiresAt of its own, so it received the entity's one by propagation
        val nameInstances = mergedEntity?.get(name) as List<*>
        assertThat(nameInstances).hasSize(2)
        val instanceFromCsr1 = nameInstances.first { (it as CompactedAttributeInstance)[NGSILD_DATASET_ID_TERM] == "1" }
            as CompactedAttributeInstance
        val instanceFromCsr2 = nameInstances.first { (it as CompactedAttributeInstance)[NGSILD_DATASET_ID_TERM] == "2" }
            as CompactedAttributeInstance

        assertEquals(moreRecentTime, instanceFromCsr1[NGSILD_EXPIRES_AT_TERM])
        assertEquals(evenMoreRecentTime, instanceFromCsr2[NGSILD_EXPIRES_AT_TERM])
    }

    @Test
    fun `mergeAttribute should discard the remote instance if it is expired`() {
        val expiredRemote = nameAttribute.plus(NGSILD_EXPIRES_AT_TERM to time)
        val merged = ContextSourceUtils.mergeAttribute(nameAttribute, expiredRemote, inclusiveCSR).getOrNull()

        assertEquals(nameAttribute, merged)
    }

    @Test
    fun `mergeAttribute should keep the remote instance if the current one is expired`() {
        val expiredCurrent = nameAttribute.plus(NGSILD_EXPIRES_AT_TERM to time)
        val merged = ContextSourceUtils.mergeAttribute(expiredCurrent, moreRecentAttribute, inclusiveCSR).getOrNull()

        assertEquals(moreRecentAttribute, merged)
    }

    @Test
    fun `mergeAttribute should fall back to the observedAt tie-break if both instances are expired`() {
        val expiredCurrent = nameAttribute.plus(NGSILD_EXPIRES_AT_TERM to time)
        val expiredRemote = moreRecentAttribute.plus(NGSILD_EXPIRES_AT_TERM to time)
        val merged = ContextSourceUtils.mergeAttribute(expiredCurrent, expiredRemote, inclusiveCSR).getOrNull()

        assertEquals(expiredRemote, merged)
    }

    @Test
    fun `mergeAttribute should apply the normal observedAt tie-break if neither instance is expired`() {
        val merged = ContextSourceUtils.mergeAttribute(nameAttribute, moreRecentAttribute, inclusiveCSR).getOrNull()

        assertEquals(moreRecentAttribute, merged)
    }
}
