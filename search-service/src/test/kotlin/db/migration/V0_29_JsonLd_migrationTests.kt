package db.migration

import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.loadSampleData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class V0_29_JsonLd_migrationTests {

    private val contexts = listOf(APIC_COMPOUND_CONTEXT)

    @Test
    fun `it should remove instances when attribute has more than one instance with the same datasetId`() {
        val payload = loadSampleData("fragments/attribute_with_two_instances_and_same_dataset_id.jsonld")
            .deserializeAsMap()

        val payloadExpanded = JsonLdUtils.expandDeserializedPayload(payload, contexts).keepOnlyOneInstanceByDatasetId()

        val expectedPayload =
            mapOf(
                "cloudCoverage" to listOf(
                    mapOf(
                        "createdAt" to "2022-07-01T07:28:21.450768829Z",
                        "datasetId" to "urn:ngsi-ld:Dataset:Total:Mean",
                        "observedAt" to "2022-07-09T00:00:00Z"
                    )
                )
            )
        val expectedPayloadExpanded = JsonLdUtils.expandDeserializedPayload(expectedPayload, contexts)

        assertEquals(expectedPayloadExpanded, payloadExpanded)
    }

    @Test
    fun `it should not remove instances when attribute has a default instance and one with a datasetId`() {
        val payload = loadSampleData("fragments/attribute_with_default_instance_and_dataset_id.jsonld")
            .deserializeAsMap()

        val payloadExpanded = JsonLdUtils.expandDeserializedPayload(payload, contexts).keepOnlyOneInstanceByDatasetId()

        val expectedPayload =
            mapOf(
                "cloudCoverage" to listOf(
                    mapOf(
                        "createdAt" to "2022-07-01T07:28:21.450768829Z",
                        "observedAt" to "2022-07-09T00:00:00Z"
                    ),
                    mapOf(
                        "createdAt" to "2022-07-01T07:28:20.450498707Z",
                        "datasetId" to "urn:ngsi-ld:Dataset:Total:Mean",
                        "observedAt" to "2022-07-03T00:00:00Z"
                    )
                )
            )
        val expectedPayloadExpanded = JsonLdUtils.expandDeserializedPayload(expectedPayload, contexts)

        assertEquals(expectedPayloadExpanded, payloadExpanded)
    }

    @Test
    fun `it should not remove instances when attribute has instances with different values for datasetId`() {
        val payload = loadSampleData("fragments/attribute_with_two_instances_and_different_dataset_id.jsonld")
            .deserializeAsMap()

        val payloadExpanded = JsonLdUtils.expandDeserializedPayload(payload, contexts).keepOnlyOneInstanceByDatasetId()

        val expectedPayload =
            mapOf(
                "cloudCoverage" to listOf(
                    mapOf(
                        "createdAt" to "2022-07-01T07:28:21.450768829Z",
                        "datasetId" to "urn:ngsi-ld:Dataset:Total:Mean",
                        "observedAt" to "2022-07-09T00:00:00Z"
                    ),
                    mapOf(
                        "createdAt" to "2022-07-01T07:28:20.450498707Z",
                        "datasetId" to "urn:ngsi-ld:Dataset:Total:Max",
                        "observedAt" to "2022-07-03T00:00:00Z"
                    )
                )
            )
        val expectedPayloadExpanded = JsonLdUtils.expandDeserializedPayload(expectedPayload, contexts)

        assertEquals(expectedPayloadExpanded, payloadExpanded)
    }

    @Test
    fun `it should remove instance when attribute has two default instances`() {
        val payload = loadSampleData("fragments/attribute_with_two_default_instances.jsonld")
            .deserializeAsMap()

        val payloadExpanded = JsonLdUtils.expandDeserializedPayload(payload, contexts).keepOnlyOneInstanceByDatasetId()

        val expectedPayload =
            mapOf(
                "cloudCoverage" to listOf(
                    mapOf(
                        "createdAt" to "2022-07-01T07:28:21.450768829Z",
                        "observedAt" to "2022-07-09T00:00:00Z"
                    ),
                )
            )
        val expectedPayloadExpanded = JsonLdUtils.expandDeserializedPayload(expectedPayload, contexts)

        assertEquals(expectedPayloadExpanded, payloadExpanded)
    }
}
