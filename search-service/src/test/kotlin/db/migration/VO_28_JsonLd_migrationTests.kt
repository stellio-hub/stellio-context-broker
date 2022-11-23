package db.migration

import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.loadSampleData
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class VO_28_JsonLd_migrationTests {

    @Test
    fun `it should remove instances when attribute have more than one instance with the same datasetId`() {
        val payload = loadSampleData("entities_with_two_instances_who_have_same_dataset_id.jsonld")
            .deserializeAsMap()
            .keepOnlyOneInstanceByDatsetId()

        val expectedPayload =
            mapOf(
                "cloudCoverage" to listOf(
                    mapOf("createdAt" to "2022-07-01T07: 28: 21.450768829Z"),
                    mapOf("datasetId" to "urn:ngsi-ld:Dataset:Total:Mean"),
                    mapOf("observedAt" to "2022-07-09T00: 00: 00Z")
                )
            )

        assertNotEquals(expectedPayload, payload)
    }
}
