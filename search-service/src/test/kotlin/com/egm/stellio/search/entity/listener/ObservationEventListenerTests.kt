package com.egm.stellio.search.entity.listener

import arrow.core.right
import com.egm.stellio.search.entity.model.NotUpdatedDetails
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.service.EntityEventService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.TEMPERATURE_PROPERTY
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.called
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.verify
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ObservationEventListener::class])
@ActiveProfiles("test")
class ObservationEventListenerTests {

    @Autowired
    private lateinit var observationEventListener: ObservationEventListener

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    private val expectedEntityId = "urn:ngsi-ld:BeeHive:01".toUri()
    private val expectedTemperatureDatasetId = "urn:ngsi-ld:Dataset:WeatherApi".toUri()

    @BeforeEach
    fun clearMocks() {
        clearAllMocks()
    }

    @Test
    fun `it should parse and transmit an ENTITY_CREATE event`() = runTest {
        val observationEvent = loadSampleData("events/entity/entityCreateEvent.json")

        coEvery {
            entityService.createEntity(any(), any(), any())
        } returns Unit.right()
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any()) } returns Job()

        observationEventListener.dispatchObservationMessage(observationEvent)

        coVerify {
            entityService.createEntity(
                any(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
        }

        coVerify(timeout = 1000L) {
            entityEventService.publishEntityCreateEvent(
                eq("0123456789-1234-5678-987654321"),
                eq(expectedEntityId),
                eq(listOf(BEEHIVE_TYPE))
            )
        }
    }

    @Test
    fun `it should parse and transmit an ATTRIBUTE_UPDATE event`() = runTest {
        val observationEvent = loadSampleData("events/entity/attributeUpdateNumericPropDatasetIdEvent.json")

        coEvery {
            entityService.partialUpdateAttribute(any(), any(), any())
        } returns UpdateResult(
            updated = listOf(TEMPERATURE_PROPERTY),
            notUpdated = emptyList()
        ).right()

        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any())
        } returns Job()

        observationEventListener.dispatchObservationMessage(observationEvent)

        coVerify {
            entityService.partialUpdateAttribute(
                expectedEntityId,
                match { it.first == TEMPERATURE_PROPERTY },
                null
            )
        }
        coVerify(timeout = 1000L) {
            entityEventService.publishAttributeChangeEvents(
                null,
                eq(expectedEntityId),
                match {
                    it.size == 1 &&
                        it[0].attributeName == TEMPERATURE_PROPERTY &&
                        it[0].datasetId == expectedTemperatureDatasetId &&
                        it[0].operationStatus == OperationStatus.UPDATED
                }
            )
        }
    }

    @Test
    fun `it should not propagate an ATTRIBUTE_UPDATE event if nothing has been updated`() = runTest {
        val observationEvent = loadSampleData("events/entity/attributeUpdateNumericPropDatasetIdEvent.json")

        coEvery {
            entityService.partialUpdateAttribute(any(), any(), any())
        } returns UpdateResult(
            emptyList(),
            listOf(NotUpdatedDetails(TEMPERATURE_PROPERTY, "Property does not exist"))
        ).right()

        observationEventListener.dispatchObservationMessage(observationEvent)

        coVerify {
            entityService.partialUpdateAttribute(any(), any(), any())
        }
        verify { entityEventService wasNot called }
    }

    @Test
    fun `it should parse and transmit an ATTRIBUTE_APPEND event`() = runTest {
        val observationEvent = loadSampleData("events/entity/attributeAppendNumericPropDatasetIdEvent.json")

        coEvery {
            entityService.appendAttributes(any(), any(), any(), any())
        } returns UpdateResult(
            listOf(TEMPERATURE_PROPERTY),
            emptyList()
        ).right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any())
        } returns Job()

        observationEventListener.dispatchObservationMessage(observationEvent)

        coVerify {
            entityService.appendAttributes(
                expectedEntityId,
                any(),
                false,
                null
            )
        }
        coVerify(timeout = 1000L) {
            entityEventService.publishAttributeChangeEvents(
                null,
                eq(expectedEntityId),
                match {
                    it.size == 1 &&
                        it[0].operationStatus == OperationStatus.APPENDED &&
                        it[0].attributeName == TEMPERATURE_PROPERTY &&
                        it[0].datasetId == expectedTemperatureDatasetId
                }
            )
        }
    }

    @Test
    fun `it should not propagate an ATTRIBUTE_APPEND event if nothing has been appended`() = runTest {
        val observationEvent = loadSampleData("events/entity/attributeAppendNumericPropDatasetIdEvent.json")

        coEvery {
            entityService.appendAttributes(any(), any(), any(), any())
        } returns UpdateResult(
            emptyList(),
            listOf(NotUpdatedDetails(TEMPERATURE_PROPERTY, "Property could not be appended"))
        ).right()

        observationEventListener.dispatchObservationMessage(observationEvent)

        verify { entityEventService wasNot called }
    }

    @Test
    fun `it should catch and drop any non compliant NGSI-LD payload`() = runTest {
        val invalidObservationEvent =
            """
            {
                "id": "urn:ngsi-ld:Entity:01"
            }
            """.trimIndent()

        assertDoesNotThrow {
            observationEventListener.dispatchObservationMessage(invalidObservationEvent)
        }

        verify { entityService wasNot called }
        verify { entityEventService wasNot called }
    }

    @Test
    fun `it should catch and drop any non compliant JSON-LD payload`() = runTest {
        val invalidObservationEvent =
            """
            {
                "id": "urn:ngsi-ld:Entity:01",,
            }
            """.trimIndent()

        assertDoesNotThrow {
            observationEventListener.dispatchObservationMessage(invalidObservationEvent)
        }

        verify { entityService wasNot called }
        verify { entityEventService wasNot called }
    }
}
