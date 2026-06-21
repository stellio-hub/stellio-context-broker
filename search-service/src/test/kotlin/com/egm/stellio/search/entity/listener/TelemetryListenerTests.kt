package com.egm.stellio.search.entity.listener

import arrow.core.right
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [TelemetryListener::class])
@ActiveProfiles("test")
class TelemetryListenerTests {

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @Autowired
    private lateinit var telemetryListener: TelemetryListener

    private val entityId = "urn:ngsi-ld:BeeHive:01".toUri()

    private fun buildTelemetryMessage(
        entityId: String = "urn:ngsi-ld:BeeHive:01",
        attributeName: String = INCOMING_IRI,
        datasetId: URI? = null,
        value: Any = 42.0
    ): String = serializeObject(
        mapOf(
            "entityId" to entityId,
            "attributeName" to attributeName,
            "value" to value,
            "observedAt" to ngsiLdDateTime().toString()
        ).let {
            if (datasetId != null)
                it.plus("datasetId" to datasetId.toString())
            else it
        }
    )

    @Test
    fun `handleTelemetryMessage should ingest a valid telemetry message`() = runTest {
        coEvery {
            entityService.mergeAttribute(any(), any(), any(), any())
        } returns listOf(
            SucceededAttributeOperationResult(INCOMING_IRI, null, OperationStatus.UPDATED, emptyMap())
        ).right()

        telemetryListener.handleTelemetryMessage(buildTelemetryMessage())

        coVerify(timeout = 1000L) {
            entityService.mergeAttribute(eq(entityId), any(), any(), any())
        }
    }

    @Test
    fun `handleTelemetryMessage should ingest a valid telemetry message having a datasetId`() = runTest {
        coEvery {
            entityService.mergeAttribute(any(), any(), any(), any())
        } returns listOf(
            SucceededAttributeOperationResult(INCOMING_IRI, null, OperationStatus.UPDATED, emptyMap())
        ).right()

        telemetryListener.handleTelemetryMessage(buildTelemetryMessage(datasetId = URI("urn:ngsi-ld:Dataset:01")))

        coVerify(timeout = 1000L) {
            entityService.mergeAttribute(eq(entityId), any(), any(), any())
        }
    }

    @Test
    fun `handleTelemetryMessage should not call ingestAttribute when message is malformed JSON`() = runTest {
        telemetryListener.handleTelemetryMessage("not valid json{")

        coVerify(exactly = 0) {
            entityService.mergeAttribute(any(), any(), any(), any())
        }
    }
}
