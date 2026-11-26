package com.egm.stellio.search.csr.util

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.support.gimmeTemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.model.NGSILD_EXPIRES_AT_TERM
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.OUTGOING_TERM
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class ContextSourceUtilsTemporalTests {

    private val localIncomingNormalized =
        deserializeObject(loadSampleData("temporal/distribution/beehive_normalized_incoming_local.jsonld"))
    private val remoteOutgoingNormalized =
        deserializeObject(loadSampleData("temporal/distribution/beehive_normalized_outgoing_remote.jsonld"))
    private val remoteIncomingNormalized =
        deserializeObject(loadSampleData("temporal/distribution/beehive_normalized_incoming_remote.jsonld"))
    private val remoteIncomingNormalized2 =
        deserializeObject(loadSampleData("temporal/distribution/beehive_normalized_incoming_remote2.jsonld"))

    private val localIncomingDatasetIdNormalized =
        deserializeObject(loadSampleData("temporal/distribution/beehive_normalized_incoming_datasetid_local.jsonld"))
    private val remoteIncomingDatasetIdNormalized =
        deserializeObject(loadSampleData("temporal/distribution/beehive_normalized_incoming_datasetid_remote.jsonld"))

    private val localIncomingSimplified =
        deserializeObject(loadSampleData("temporal/distribution/beehive_simplified_incoming_local.jsonld"))
    private val remoteOutgoingSimplified =
        deserializeObject(loadSampleData("temporal/distribution/beehive_simplified_outgoing_remote.jsonld"))
    private val remoteIncomingSimplified =
        deserializeObject(loadSampleData("temporal/distribution/beehive_simplified_incoming_remote.jsonld"))

    private val localIncomingDatasetIdSimplified =
        deserializeObject(loadSampleData("temporal/distribution/beehive_simplified_incoming_datasetid_local.jsonld"))
    private val remoteIncomingDatasetIdSimplified =
        deserializeObject(loadSampleData("temporal/distribution/beehive_simplified_incoming_datasetid_remote.jsonld"))
    private val remoteIncomingDatasetIdSimplified2 =
        deserializeObject(loadSampleData("temporal/distribution/beehive_simplified_incoming_datasetid_remote2.jsonld"))

    private val localIncomingAggregated =
        deserializeObject(loadSampleData("temporal/distribution/beehive_aggregated_incoming_local.jsonld"))
    private val remoteOutgoingAggregated =
        deserializeObject(loadSampleData("temporal/distribution/beehive_aggregated_outgoing_remote.jsonld"))
    private val remoteIncomingAggregated =
        deserializeObject(loadSampleData("temporal/distribution/beehive_aggregated_incoming_remote.jsonld"))

    private val localIncomingDatasetIdAggregated =
        deserializeObject(loadSampleData("temporal/distribution/beehive_aggregated_incoming_datasetid_local.jsonld"))
    private val remoteIncomingDatasetIdAggregated =
        deserializeObject(loadSampleData("temporal/distribution/beehive_aggregated_incoming_datasetid_remote.jsonld"))
    private val remoteIncomingDatasetIdAggregated2 =
        deserializeObject(loadSampleData("temporal/distribution/beehive_aggregated_incoming_datasetid_remote2.jsonld"))

    private val localScopeTemporal =
        deserializeObject(loadSampleData("temporal/distribution/beehive_simplified_scope_local.jsonld"))
    private val remoteScopeTemporal =
        deserializeObject(loadSampleData("temporal/distribution/beehive_simplified_scope_remote.jsonld"))

    private val inclusiveCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri())
    private val auxiliaryCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri(), mode = Mode.AUXILIARY)

    private val moreRecentTime = "2020-01-01T01:01:01.01Z"
    private val evenMoreRecentTime = "2030-01-01T01:01:01.01Z"

    @ParameterizedTest
    @CsvSource(
        "NORMALIZED,default", "NORMALIZED,firstN", "NORMALIZED,lastN",
        "TEMPORAL_VALUES,default", "TEMPORAL_VALUES,firstN", "TEMPORAL_VALUES,lastN",
        "AGGREGATED_VALUES,default", "AGGREGATED_VALUES,firstN", "AGGREGATED_VALUES,lastN"
    )
    fun `mergeTemporalEntities should order merged instances by time`(
        representation: TemporalRepresentation,
        pageParameter: String
    ) {
        val wholeSecond = "2020-02-29T01:00:00Z"
        val tenth = "2020-02-29T01:00:00.1Z"
        val sameTenth = "2020-02-29T01:00:00.100Z"
        val laterFraction = "2020-02-29T01:00:00.100001Z"
        val local = temporalEntity(representation, wholeSecond, tenth)
        val remote = temporalEntity(representation, sameTenth, laterFraction)
        val query = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery().copy(
                firstN = 2.takeIf { pageParameter == "firstN" },
                lastN = 2.takeIf { pageParameter == "lastN" }
            ),
            representation
        )
        val expected = if (pageParameter == "lastN")
            temporalEntity(representation, laterFraction, tenth, sameTenth, wholeSecond)
        else
            temporalEntity(representation, wholeSecond, tenth, sameTenth, laterFraction)

        val retrieved = ContextSourceUtils.mergeTemporalEntities(local, listOf(remote to inclusiveCSR), query)
        val queried = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(local),
            listOf(listOf(remote) to inclusiveCSR),
            query
        )

        assertTrue(retrieved.isRight())
        assertTrue(queried.isRight())
        assertJsonPayloadsAreEqual(serializeObject(expected), serializeObject(requireNotNull(retrieved.getOrNull())))
        assertJsonPayloadsAreEqual(
            serializeObject(listOf(expected)),
            serializeObject(requireNotNull(queried.getOrNull()))
        )
    }

    @Test
    fun `mergeTemporalEntities should compare timestamp text without parsing it`() {
        val representation = TemporalRepresentation.NORMALIZED
        val localTime = "2020-02-29T01:00:00Z"
        val remoteTime = "2020-02-30T01:00:00Z"
        val result = ContextSourceUtils.mergeTemporalEntities(
            temporalEntity(representation, localTime),
            listOf(temporalEntity(representation, remoteTime) to inclusiveCSR),
            createTemporalQueryWithRepresentation(representation)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            serializeObject(temporalEntity(representation, localTime, remoteTime)),
            serializeObject(requireNotNull(result.getOrNull()))
        )
    }

    @Test
    fun `mergeTemporalEntities should merge normalized instances with different attributes`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingNormalized,
            listOf(remoteOutgoingNormalized to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_normalized_incoming_outgoing.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge normalized instances with the same attribute`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingNormalized,
            listOf(remoteIncomingNormalized to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_normalized_incoming.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should not merge normalized instances with the same observedAt and datasetId`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingDatasetIdNormalized,
            listOf(remoteIncomingDatasetIdNormalized to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_normalized_incoming_datasetid.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge normalized instances from multiple CSRs with different modes`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingNormalized,
            listOf(remoteIncomingNormalized to inclusiveCSR, remoteIncomingNormalized2 to auxiliaryCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_normalized_incoming_two_csrs.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge simplified instances with different attributes`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingSimplified,
            listOf(remoteOutgoingSimplified to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.TEMPORAL_VALUES)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_simplified_incoming_outgoing.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge simplified instances with the same attribute`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingSimplified,
            listOf(remoteIncomingSimplified to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.TEMPORAL_VALUES)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_simplified_incoming.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge simplified instances with different datasetIds from multiple CSRs`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingDatasetIdSimplified,
            listOf(
                remoteIncomingDatasetIdSimplified to inclusiveCSR,
                remoteIncomingDatasetIdSimplified2 to auxiliaryCSR
            ),
            createTemporalQueryWithRepresentation(TemporalRepresentation.TEMPORAL_VALUES)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_simplified_incoming_datasetid.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge simplified instances with datasetId only on remote`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingSimplified,
            listOf(remoteIncomingDatasetIdSimplified to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.TEMPORAL_VALUES)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_simplified_incoming_datasetid_remote.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge aggregated instances with different attributes`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingAggregated,
            listOf(remoteOutgoingAggregated to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.AGGREGATED_VALUES)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_aggregated_incoming_outgoing.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge aggregated instances for the same attribute`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingAggregated,
            listOf(remoteIncomingAggregated to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.AGGREGATED_VALUES)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_aggregated_incoming.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge aggregated instances with different datasetIds from multiple CSRs`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingDatasetIdAggregated,
            listOf(
                remoteIncomingDatasetIdAggregated to inclusiveCSR,
                remoteIncomingDatasetIdAggregated2 to auxiliaryCSR
            ),
            createTemporalQueryWithRepresentation(TemporalRepresentation.AGGREGATED_VALUES)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_aggregated_incoming_datasetid.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should merge simplified instances of scope`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localScopeTemporal,
            listOf(remoteScopeTemporal to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.TEMPORAL_VALUES)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/distribution/expectations/beehive_simplified_scope.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should return null when there is no local entity and no remote entities`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            null,
            emptyList(),
            createTemporalQueryWithRepresentation(TemporalRepresentation.AGGREGATED_VALUES)
        )

        assertTrue(result.isRight())
        assertNull(result.getOrNull())
    }

    @Test
    fun `mergeTemporalEntities should return remote entity when there is no local entity`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            null,
            listOf(remoteOutgoingNormalized to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.AGGREGATED_VALUES)
        )

        assertTrue(result.isRight())
        assertEquals(remoteOutgoingNormalized, result.getOrNull())
    }

    @Test
    fun `mergeTemporalEntities should merge multiple remote entities when there is no local entity`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            null,
            listOf(remoteIncomingNormalized to inclusiveCSR, remoteOutgoingNormalized to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertNotNull(result.getOrNull()!![INCOMING_TERM])
        assertNotNull(result.getOrNull()!![OUTGOING_TERM])
    }

    @Test
    fun `mergeTemporalEntities should drop expiresAt if one source is missing it`() {
        val localWithExpiresAt = localIncomingNormalized.plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)
        val result = ContextSourceUtils.mergeTemporalEntities(
            localWithExpiresAt,
            listOf(remoteIncomingNormalized to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertThat(result.getOrNull()).doesNotContainKey(NGSILD_EXPIRES_AT_TERM)
    }

    @Test
    fun `mergeTemporalEntities should keep the furthest in the future expiresAt if all sources have one`() {
        val localWithExpiresAt = localIncomingNormalized.plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)
        val remoteWithLaterExpiresAt = remoteIncomingNormalized.plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val result = ContextSourceUtils.mergeTemporalEntities(
            localWithExpiresAt,
            listOf(remoteWithLaterExpiresAt to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertEquals(evenMoreRecentTime, result.getOrNull()?.get(NGSILD_EXPIRES_AT_TERM))
    }

    @Test
    fun `mergeTemporalEntitiesLists should drop expiresAt if one source is missing it`() {
        val localWithExpiresAt = localIncomingNormalized.plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)
        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(localWithExpiresAt),
            listOf(listOf(remoteIncomingNormalized) to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertThat(result.getOrNull()!!.first()).doesNotContainKey(NGSILD_EXPIRES_AT_TERM)
    }

    @Test
    fun `mergeTemporalEntitiesLists should keep the furthest in the future expiresAt if all sources have one`() {
        val localWithExpiresAt = localIncomingNormalized.plus(NGSILD_EXPIRES_AT_TERM to moreRecentTime)
        val remoteWithLaterExpiresAt = remoteIncomingNormalized.plus(NGSILD_EXPIRES_AT_TERM to evenMoreRecentTime)
        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(localWithExpiresAt),
            listOf(listOf(remoteWithLaterExpiresAt) to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertEquals(evenMoreRecentTime, result.getOrNull()!!.first()[NGSILD_EXPIRES_AT_TERM])
    }

    @Test
    fun `mergeTemporalEntitiesLists should add remote entities with ids absent locally`() {
        val remoteEntity = remoteOutgoingNormalized.toMutableMap()
            .also { it[NGSILD_ID_TERM] = "urn:ngsi-ld:BeeHive:REMOTE" }

        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(localIncomingNormalized),
            listOf(listOf(remoteEntity) to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertThat(result.getOrNull()!!).hasSize(2)
    }

    private fun createTemporalQueryWithRepresentation(
        temporalRepresentation: TemporalRepresentation
    ): TemporalEntitiesQueryFromGet =
        gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(),
            temporalRepresentation
        )

    private fun temporalEntity(representation: TemporalRepresentation, vararg times: String): Map<String, Any> {
        val attribute = when (representation) {
            TemporalRepresentation.NORMALIZED -> times.map {
                mapOf("type" to "Property", "value" to 1, "observedAt" to it)
            }
            TemporalRepresentation.TEMPORAL_VALUES ->
                mapOf("type" to "Property", "values" to times.map { listOf(1, it) })
            TemporalRepresentation.AGGREGATED_VALUES ->
                mapOf("type" to "Property", "min" to times.map { listOf(1, it, "2020-01-01T02:00:00Z") })
        }
        return mapOf(NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC", "type" to "BeeHive", INCOMING_TERM to attribute)
    }
}
