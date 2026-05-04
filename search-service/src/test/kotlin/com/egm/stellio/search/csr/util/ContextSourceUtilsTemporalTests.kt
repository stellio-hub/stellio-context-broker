package com.egm.stellio.search.csr.util

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.support.gimmeTemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.util.TemporalRepresentation
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
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class ContextSourceUtilsTemporalTests {

    private val localIncomingNormalized =
        deserializeObject(loadSampleData("temporal/beehive_normalized_incoming_local.jsonld"))
    private val remoteOutgoingNormalized =
        deserializeObject(loadSampleData("temporal/beehive_normalized_outgoing_remote.jsonld"))
    private val remoteIncomingNormalized =
        deserializeObject(loadSampleData("temporal/beehive_normalized_incoming_remote.jsonld"))
    private val remoteIncomingNormalized2 =
        deserializeObject(loadSampleData("temporal/beehive_normalized_incoming_remote2.jsonld"))

    private val localIncomingSimplified =
        deserializeObject(loadSampleData("temporal/beehive_simplified_incoming_local.jsonld"))
    private val remoteOutgoingSimplified =
        deserializeObject(loadSampleData("temporal/beehive_simplified_outgoing_remote.jsonld"))
    private val remoteIncomingSimplified =
        deserializeObject(loadSampleData("temporal/beehive_simplified_incoming_remote.jsonld"))

    private val localIncomingDatasetIdSimplified =
        deserializeObject(loadSampleData("temporal/beehive_simplified_incoming_datasetid_local.jsonld"))
    private val remoteIncomingDatasetIdSimplified =
        deserializeObject(loadSampleData("temporal/beehive_simplified_incoming_datasetid_remote.jsonld"))
    private val remoteIncomingDatasetIdSimplified2 =
        deserializeObject(loadSampleData("temporal/beehive_simplified_incoming_datasetid_remote2.jsonld"))

    private val localIncomingAggregated =
        deserializeObject(loadSampleData("temporal/beehive_aggregated_incoming_local.jsonld"))
    private val remoteOutgoingAggregated =
        deserializeObject(loadSampleData("temporal/beehive_aggregated_outgoing_remote.jsonld"))
    private val remoteIncomingAggregated =
        deserializeObject(loadSampleData("temporal/beehive_aggregated_incoming_remote.jsonld"))

    private val localIncomingDatasetIdAggregated =
        deserializeObject(loadSampleData("temporal/beehive_aggregated_incoming_datasetid_local.jsonld"))
    private val remoteIncomingDatasetIdAggregated =
        deserializeObject(loadSampleData("temporal/beehive_aggregated_incoming_datasetid_remote.jsonld"))
    private val remoteIncomingDatasetIdAggregated2 =
        deserializeObject(loadSampleData("temporal/beehive_aggregated_incoming_datasetid_remote2.jsonld"))

    private val localScopeTemporal =
        deserializeObject(loadSampleData("temporal/beehive_simplified_scope_local.jsonld"))
    private val remoteScopeTemporal =
        deserializeObject(loadSampleData("temporal/beehive_simplified_scope_remote.jsonld"))

    private val inclusiveCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri())
    private val auxiliaryCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri(), mode = Mode.AUXILIARY)

    @Test
    fun `mergeTemporalEntities should merge normalized instances with different attributes`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localIncomingNormalized,
            listOf(remoteOutgoingNormalized to inclusiveCSR),
            createTemporalQueryWithRepresentation(TemporalRepresentation.NORMALIZED)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/expectations/beehive_normalized_incoming_outgoing.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_normalized_incoming.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_normalized_incoming_two_csrs.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_simplified_incoming_outgoing.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_simplified_incoming.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_simplified_incoming_datasetid.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_simplified_incoming_datasetid_remote.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_aggregated_incoming_outgoing.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_aggregated_incoming.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_aggregated_incoming_datasetid.jsonld"),
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
            loadSampleData("temporal/expectations/beehive_simplified_scope.jsonld"),
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
}
