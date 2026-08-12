package com.egm.stellio.search.service.execution.model

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ServiceExecutionTests {
    @Test
    fun `deserialize should parse a simple service execution`() = runTest {
        val execution = ServiceExecution.deserialize(
            """
            {
              "id": "urn:ngsi-ld:ServiceExecution:4673",
              "type": "ServiceExecution",
              "serviceId": "urn:ngsi-ld:ServiceRegistration:sr3689",
              "entityId": "urn:ngsi-ld:Light:001",
              "entityType": "Light",
              "serviceName": "setBrightness",
              "input": {
                "brightness": 125,
                "transition": {
                  "duration": 2
                }
              },
              "executionStatus": "pending",
              "completion": 0.25,
              "responseStatusCode": 202
            }
            """.trimIndent().deserializeAsMap(),
            emptyList()
        ).shouldSucceedAndResult()

        assertEquals("urn:ngsi-ld:ServiceExecution:4673", execution.id.toString())
        assertEquals("urn:ngsi-ld:ServiceRegistration:sr3689", execution.serviceId.toString())
        assertEquals("urn:ngsi-ld:Light:001", execution.entityId.toString())
        assertEquals("Light", execution.entityType)
        assertEquals("setBrightness", execution.serviceName)
        val input = execution.input as Map<*, *>
        assertEquals(125, input["brightness"])
        assertThat(input["transition"]).isEqualTo(mapOf("duration" to 2))
        assertEquals(ServiceExecutionStatus.PENDING, execution.executionStatus)
        assertEquals(0.25, execution.completion)
        assertEquals(202, execution.responseStatusCode)
        execution.validate().shouldSucceed()
    }

    @Test
    fun `deserialize should accept direct string and integer input`() = runTest {
        listOf(
            "\"turn-on\"" to "turn-on",
            "125" to 125
        ).forEach { (serializedInput, expectedInput) ->
            val execution = ServiceExecution.deserialize(
                """
                {
                  "serviceId": "urn:ngsi-ld:ServiceRegistration:sr3689",
                  "entityId": "urn:ngsi-ld:Light:001",
                  "entityType": "Light",
                  "input": $serializedInput
                }
                """.trimIndent().deserializeAsMap(),
                emptyList()
            ).shouldSucceedAndResult()

            assertEquals(expectedInput, execution.input)
            assertEquals(null, execution.serviceName)
            assertEquals(null, execution.completion)
            assertEquals(null, execution.responseStatusCode)
        }
    }

    @Test
    fun `validate should reject completion outside proportion range`() = runTest {
        val execution = ServiceExecution(
            serviceId = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri(),
            entityId = "urn:ngsi-ld:Light:001".toUri(),
            entityType = "Light",
            input = 125
        )

        listOf(-0.1, 1.1, Double.NaN, Double.POSITIVE_INFINITY).forEach { completion ->
            execution.copy(completion = completion).validate().shouldFailWith {
                it is BadRequestDataException && it.message.contains("completion")
            }
        }
    }

    @Test
    fun `deserialize should reject an unsupported execution status`() = runTest {
        ServiceExecution.deserialize(
            """
            {
              "serviceId": "urn:ngsi-ld:ServiceRegistration:sr3689",
              "entityId": "urn:ngsi-ld:Light:001",
              "entityType": "Light",
              "serviceName": "setBrightness",
              "input": {},
              "executionStatus": "unsupported"
            }
            """.trimIndent().deserializeAsMap(),
            emptyList()
        ).shouldFailWith {
            it is BadRequestDataException &&
                it.message.startsWith("Service execution cannot be parsed: Cannot deserialize value of type") &&
                it.message.contains("ServiceExecutionStatus") &&
                it.message.contains("unsupported")
        }
    }

    @Test
    fun `mergeWithFragment should update executor-controlled members`() = runTest {
        val execution = ServiceExecution(
            serviceId = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri(),
            entityId = "urn:ngsi-ld:Light:001".toUri(),
            entityType = "Light",
            serviceName = "setBrightness",
            input = mapOf("brightness" to 125)
        )

        val updated = execution.mergeWithFragment(
            mapOf(
                "executionStatus" to "success",
                "completion" to 1.0,
                "output" to "Brightness successfully changed."
            ),
            emptyList()
        ).shouldSucceedAndResult()

        assertEquals(ServiceExecutionStatus.SUCCESS, updated.executionStatus)
        assertEquals(1.0, updated.completion)
        assertEquals("Brightness successfully changed.", updated.output)
        assertThat(updated.modifiedAt).isAfterOrEqualTo(execution.modifiedAt)
    }

    @Test
    fun `mergeWithFragment should reject non executor-controlled members`() = runTest {
        val execution = ServiceExecution(
            serviceId = "urn:ngsi-ld:ServiceRegistration:sr3689".toUri(),
            entityId = "urn:ngsi-ld:Light:001".toUri(),
            entityType = "Light",
            input = 125
        )

        listOf(
            "id",
            "type",
            "serviceId",
            "entityId",
            "entityType",
            "input",
            "serviceName",
            "createdAt",
            "modifiedAt"
        ).forEach { member ->
            execution.mergeWithFragment(mapOf(member to "unsupported"), emptyList()).shouldFailWith {
                it is BadRequestDataException &&
                    it.message.contains("'completion', 'output' and 'executionStatus'")
            }
        }
    }
}
