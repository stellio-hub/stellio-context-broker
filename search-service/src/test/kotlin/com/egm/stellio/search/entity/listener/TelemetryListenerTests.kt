package com.egm.stellio.search.entity.listener

import arrow.core.right
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_LOCATION_IRI
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.slot
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
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
        attributeType: URI? = null,
        datasetId: URI? = null,
        value: Any = 42.0
    ): String = serializeObject(
        mapOf(
            "entityId" to entityId,
            "attributeName" to attributeName,
            "value" to value,
            "observedAt" to ngsiLdDateTime().toString()
        ).let {
            if (attributeType != null)
                it.plus("attributeType" to attributeType.toString())
            else it
        }.let {
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
    fun `handleTelemetryMessage should ingest a valid telemetry message with a geo location value`() = runTest {
        val fragmentSlot = slot<ExpandedAttributeInstance>()
        coEvery {
            entityService.mergeAttribute(any(), any(), capture(fragmentSlot), any())
        } returns listOf(
            SucceededAttributeOperationResult(NGSILD_LOCATION_IRI, null, OperationStatus.UPDATED, emptyMap())
        ).right()

        val geoValue = mapOf(
            "type" to "Point",
            "coordinates" to listOf(24.30623, 60.07966)
        )
        telemetryListener.handleTelemetryMessage(
            buildTelemetryMessage(
                attributeName = NGSILD_LOCATION_IRI,
                attributeType = URI(NGSILD_GEOPROPERTY_TYPE.uri),
                value = geoValue
            )
        )

        coVerify(timeout = 1000L) {
            entityService.mergeAttribute(eq(entityId), eq(NGSILD_LOCATION_IRI), any(), any())
        }

        val fragment = fragmentSlot.captured
        assertEquals(listOf(NGSILD_GEOPROPERTY_TYPE.uri), fragment[JSONLD_TYPE_KW])
        val wktValue = (fragment[NGSILD_PROPERTY_VALUE] as List<*>)
            .first()
            .let { it as Map<*, *> }[JSONLD_VALUE_KW]
        assertTrue(wktValue is String && (wktValue as String).startsWith("POINT"))
    }

    @Test
    fun `handleTelemetryMessage should not call mergeAttribute when geo value is invalid GeoJSON`() = runTest {
        telemetryListener.handleTelemetryMessage(
            buildTelemetryMessage(
                attributeName = NGSILD_LOCATION_IRI,
                attributeType = URI(NGSILD_GEOPROPERTY_TYPE.uri),
                value = mapOf("not" to "geojson")
            )
        )
        coVerify(exactly = 0) {
            entityService.mergeAttribute(any(), any(), any(), any())
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
