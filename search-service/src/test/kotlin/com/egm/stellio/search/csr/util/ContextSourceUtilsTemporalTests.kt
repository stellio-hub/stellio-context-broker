package com.egm.stellio.search.csr.util

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
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

    private val localEntityNormalized: Map<String, Any> =
        deserializeObject(loadSampleData("temporal/beehive_normalized_incoming.jsonld"))
    private val remoteEntityNormalized: Map<String, Any> =
        deserializeObject(loadSampleData("temporal/beehive_normalized_outgoing.jsonld"))

    private val inclusiveCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri())
    private val auxiliaryCSR = ContextSourceRegistration(endpoint = "http://mock-uri".toUri(), mode = Mode.AUXILIARY)

    @Test
    fun `mergeTemporalEntities should merge a normalized local entity with remote entity`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            localEntityNormalized,
            listOf(remoteEntityNormalized to inclusiveCSR)
        )

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/expectations/beehive_normalized_local_remote_inclusive.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should concatenate normalized instances for the same attribute from inclusive CSR`() {
        val remoteIncoming = deserializeObject(
            loadSampleData("temporal/beehive_normalized_incoming_remote.jsonld")
        )

        val result = ContextSourceUtils.mergeTemporalEntities(
            localEntityNormalized,
            listOf(remoteIncoming to inclusiveCSR)
        )

        assertTrue(result.isRight())
        assertThat(result.getOrNull()!!["incoming"] as List<*>).hasSize(5)
    }

    @Test
    fun `mergeTemporalEntities should concatenate normalized instances for the same attribute from auxiliary CSR`() {
        val remoteIncoming = deserializeObject(
            loadSampleData("temporal/beehive_normalized_incoming_remote.jsonld")
        )

        val result = ContextSourceUtils.mergeTemporalEntities(
            localEntityNormalized,
            listOf(remoteIncoming to auxiliaryCSR)
        )

        assertTrue(result.isRight())
        // auxiliary CSR does not suppress temporal instance concatenation (unlike non-temporal merge)
        assertThat(result.getOrNull()!!["incoming"] as List<*>).hasSize(5)
    }

    @Test
    fun `mergeTemporalEntities should concatenate instances from multiple CSRs with different modes`() {
        val remoteInclusiveIncoming = deserializeObject(
            loadSampleData("temporal/beehive_normalized_incoming_remote.jsonld")
        )
        val remoteAuxiliaryIncoming = deserializeObject(
            loadSampleData("temporal/beehive_normalized_incoming_remote2.jsonld")
        )

        val result = ContextSourceUtils.mergeTemporalEntities(
            localEntityNormalized,
            listOf(remoteInclusiveIncoming to inclusiveCSR, remoteAuxiliaryIncoming to auxiliaryCSR)
        )

        assertTrue(result.isRight())
        // 3 local + 2 inclusive remote + 2 auxiliary remote
        assertThat(result.getOrNull()!!["incoming"] as List<*>).hasSize(7)
    }

    @Test
    fun `mergeTemporalEntities should merge temporal values entities with different attributes`() {
        val local = deserializeObject(loadSampleData("temporal/beehive_temporal_values_incoming.jsonld"))
        val remote = deserializeObject(loadSampleData("temporal/beehive_temporal_values_outgoing.jsonld"))

        val result = ContextSourceUtils.mergeTemporalEntities(local, listOf(remote to inclusiveCSR))

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/expectations/beehive_temporal_values_local_remote_inclusive.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should concatenate temporal values instances for the same attribute`() {
        val local = deserializeObject(loadSampleData("temporal/beehive_temporal_values_incoming.jsonld"))
        val remote = deserializeObject(loadSampleData("temporal/beehive_temporal_values_incoming_remote.jsonld"))

        val result = ContextSourceUtils.mergeTemporalEntities(local, listOf(remote to inclusiveCSR))

        assertTrue(result.isRight())
        assertJsonPayloadsAreEqual(
            loadSampleData("temporal/expectations/beehive_temporal_values_same_attr_merged.jsonld"),
            serializeObject(result.getOrNull()!!)
        )
    }

    @Test
    fun `mergeTemporalEntities should concatenate aggregated instances for the same attribute`() {
        val incomingAggLocal = mapOf(
            "type" to "Property",
            "avg" to listOf(listOf(2.5, "2020-01-24T12:00:00.000Z", "2020-01-24T13:00:00.000Z"))
        )
        val incomingAggRemote = mapOf(
            "type" to "Property",
            "avg" to listOf(listOf(3.1, "2020-01-24T13:00:00.000Z", "2020-01-24T14:00:00.000Z"))
        )
        val local = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            INCOMING_TERM to incomingAggLocal
        )
        val remote = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            INCOMING_TERM to incomingAggRemote
        )

        val result = ContextSourceUtils.mergeTemporalEntities(local, listOf(remote to inclusiveCSR))

        assertTrue(result.isRight())
        assertThat(result.getOrNull()!![INCOMING_TERM] as List<*>)
            .hasSize(2)
            .containsExactly(incomingAggLocal, incomingAggRemote)
    }

    @Test
    fun `mergeTemporalEntities should merge aggregated entities with different attributes`() {
        val incomingAgg = mapOf(
            "type" to "Property",
            "avg" to listOf(listOf(2.5, "2020-01-24T12:00:00.000Z", "2020-01-24T13:00:00.000Z"))
        )
        val outgoingAgg = mapOf(
            "type" to "Property",
            "sum" to listOf(listOf(100, "2020-01-24T12:00:00.000Z", "2020-01-24T13:00:00.000Z"))
        )
        val local = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            INCOMING_TERM to incomingAgg
        )
        val remote = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            OUTGOING_TERM to outgoingAgg
        )

        val result = ContextSourceUtils.mergeTemporalEntities(local, listOf(remote to inclusiveCSR))

        assertTrue(result.isRight())
        assertEquals(incomingAgg, result.getOrNull()!![INCOMING_TERM])
        assertEquals(outgoingAgg, result.getOrNull()!![OUTGOING_TERM])
    }

    @Test
    fun `mergeTemporalEntities should merge scope temporal instances from local and remote`() {
        val scopeInstance1 = mapOf(
            "type" to "Property",
            "values" to listOf("/A/B", "2020-01-24T12:01:22.066Z")
        )
        val scopeInstance2 = mapOf(
            "type" to "Property",
            "values" to listOf("/C/D", "2020-01-24T13:01:22.066Z")
        )
        val remoteScopeInstance = mapOf(
            "type" to "Property",
            "values" to listOf("/E/F", "2020-01-24T14:01:22.066Z")
        )
        val local = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            NGSILD_SCOPE_TERM to listOf(scopeInstance1, scopeInstance2)
        )
        val remote = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            NGSILD_SCOPE_TERM to listOf(remoteScopeInstance)
        )

        val result = ContextSourceUtils.mergeTemporalEntities(local, listOf(remote to inclusiveCSR))

        assertTrue(result.isRight())
        assertThat(result.getOrNull()!![NGSILD_SCOPE_TERM] as List<*>)
            .hasSize(3)
            .containsExactlyInAnyOrder(scopeInstance1, scopeInstance2, remoteScopeInstance)
    }

    @Test
    fun `mergeTemporalEntities should return null when there is no local entity and no remote entities`() {
        val result = ContextSourceUtils.mergeTemporalEntities(null, emptyList())

        assertTrue(result.isRight())
        assertNull(result.getOrNull())
    }

    @Test
    fun `mergeTemporalEntities should return remote entity when there is no local entity`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            null,
            listOf(remoteEntityNormalized to inclusiveCSR)
        )

        assertTrue(result.isRight())
        assertEquals(remoteEntityNormalized, result.getOrNull())
    }

    @Test
    fun `mergeTemporalEntities should merge multiple remote entities when there is no local entity`() {
        val result = ContextSourceUtils.mergeTemporalEntities(
            null,
            listOf(localEntityNormalized to inclusiveCSR, remoteEntityNormalized to inclusiveCSR)
        )

        assertTrue(result.isRight())
        assertNotNull(result.getOrNull()!!["incoming"])
        assertNotNull(result.getOrNull()!!["outgoing"])
    }

    @Test
    fun `mergeTemporalEntities should concatenate temporal instances with different datasetIds`() {
        val instanceDatasetA = mapOf(
            "type" to "Property",
            "value" to 1550,
            "observedAt" to "2020-01-24T12:01:22.066Z",
            "datasetId" to "urn:ngsi-ld:Dataset:A",
            "instanceId" to "urn:ngsi-ld:Instance:001"
        )
        val instanceDatasetB = mapOf(
            "type" to "Property",
            "value" to 2000,
            "observedAt" to "2020-01-24T15:01:22.066Z",
            "datasetId" to "urn:ngsi-ld:Dataset:B",
            "instanceId" to "urn:ngsi-ld:Instance:002"
        )
        val local = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            INCOMING_TERM to listOf(instanceDatasetA)
        )
        val remote = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            INCOMING_TERM to listOf(instanceDatasetB)
        )

        val result = ContextSourceUtils.mergeTemporalEntities(local, listOf(remote to inclusiveCSR))

        assertTrue(result.isRight())
        assertThat(result.getOrNull()!![INCOMING_TERM] as List<*>)
            .hasSize(2)
            .containsExactlyInAnyOrder(instanceDatasetA, instanceDatasetB)
    }

    @Test
    fun `mergeTemporalEntities should concatenate temporal instances with the same datasetId from different sources`() {
        val localInstance = mapOf(
            "type" to "Property",
            "value" to 1550,
            "observedAt" to "2020-01-24T12:01:22.066Z",
            "datasetId" to "urn:ngsi-ld:Dataset:A",
            "instanceId" to "urn:ngsi-ld:Instance:001"
        )
        val remoteInstance = mapOf(
            "type" to "Property",
            "value" to 2000,
            "observedAt" to "2020-01-24T15:01:22.066Z",
            "datasetId" to "urn:ngsi-ld:Dataset:A",
            "instanceId" to "urn:ngsi-ld:Instance:002"
        )
        val local = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            INCOMING_TERM to listOf(localInstance)
        )
        val remote = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTC",
            NGSILD_TYPE_TERM to "BeeHive",
            INCOMING_TERM to listOf(remoteInstance)
        )

        val result = ContextSourceUtils.mergeTemporalEntities(local, listOf(remote to inclusiveCSR))

        assertTrue(result.isRight())
        // unlike non-temporal merge, same datasetId instances from different sources are both kept
        assertThat(result.getOrNull()!![INCOMING_TERM] as List<*>)
            .hasSize(2)
            .containsExactlyInAnyOrder(localInstance, remoteInstance)
    }

    @Test
    fun `mergeTemporalEntitiesLists should merge temporal entities from multiple CSRs`() {
        val remoteIncoming = deserializeObject(
            loadSampleData("temporal/beehive_normalized_incoming_remote.jsonld")
        )

        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(localEntityNormalized),
            listOf(
                listOf(remoteIncoming) to inclusiveCSR,
                listOf(remoteEntityNormalized) to auxiliaryCSR
            )
        )

        assertTrue(result.isRight())
        val entities = result.getOrNull()!!
        assertThat(entities).hasSize(1)
        assertThat(entities.first()["incoming"] as List<*>).hasSize(5)
        assertNotNull(entities.first()["outgoing"])
    }

    @Test
    fun `mergeTemporalEntitiesLists should add remote entities with ids absent locally`() {
        val remoteEntity = remoteEntityNormalized.toMutableMap()
            .also { it[NGSILD_ID_TERM] = "urn:ngsi-ld:BeeHive:REMOTE" }

        val result = ContextSourceUtils.mergeTemporalEntitiesLists(
            listOf(localEntityNormalized),
            listOf(listOf(remoteEntity) to inclusiveCSR)
        )

        assertTrue(result.isRight())
        assertThat(result.getOrNull()!!).hasSize(2)
    }
}
