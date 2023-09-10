package com.egm.stellio.search.listener

import arrow.core.right
import com.egm.stellio.search.model.NotUpdatedDetails
import com.egm.stellio.search.model.UpdateOperationResult
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.model.UpdatedDetails
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ObservationEventListener::class])
@ActiveProfiles("test")
class ObservationEventListenerTests {

    @Autowired
    private lateinit var observationEventListener: ObservationEventListener

    @MockkBean(relaxed = true)
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    private val expectedEntityId = "urn:ngsi-ld:BeeHive:01".toUri()
    private val expectedTemperatureDatasetId = "urn:ngsi-ld:Dataset:WeatherApi".toUri()

    @Test
    fun `it should parse and transmit an ENTITY_CREATE event`() = runTest {
        val observationEvent = loadSampleData("events/entity/entityCreateEvent.json")

        coEvery {
            entityPayloadService.createEntity(any<String>(), any(), any())
        } returns Unit.right()
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } returns Job()

        observationEventListener.dispatchObservationMessage(observationEvent)

        coVerify {
            entityPayloadService.createEntity(
                any(),
                listOf(APIC_COMPOUND_CONTEXT),
                eq("0123456789-1234-5678-987654321")
            )
        }

        coVerify(timeout = 1000L) {
            entityEventService.publishEntityCreateEvent(
                eq("0123456789-1234-5678-987654321"),
                eq(expectedEntityId),
                eq(listOf(BEEHIVE_TYPE)),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `it should parse and transmit an ATTRIBUTE_UPDATE event`() = runTest {
        val observationEvent = loadSampleData("events/entity/attributeUpdateNumericPropDatasetIdEvent.json")

        coEvery {
            entityPayloadService.partialUpdateAttribute(any(), any(), any())
        } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    TEMPERATURE_PROPERTY,
                    expectedTemperatureDatasetId,
                    UpdateOperationResult.UPDATED
                )
            ),
            notUpdated = arrayListOf()
        ).right()

        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any(), any())
        } returns Job()

        observationEventListener.dispatchObservationMessage(observationEvent)

        coVerify {
            entityPayloadService.partialUpdateAttribute(
                expectedEntityId,
                match { it.first == TEMPERATURE_PROPERTY },
                null
            )
        }
        coVerify(timeout = 1000L) {
            entityEventService.publishAttributeChangeEvents(
                null,
                eq(expectedEntityId),
                match { it.containsKey(TEMPERATURE_PROPERTY) },
                match {
                    it.updated.size == 1 &&
                        it.updated[0].attributeName == TEMPERATURE_PROPERTY &&
                        it.updated[0].datasetId == expectedTemperatureDatasetId &&
                        it.updated[0].updateOperationResult == UpdateOperationResult.UPDATED
                },
                eq(false),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `it should not propagate an ATTRIBUTE_UPDATE event if nothing has been updated`() = runTest {
        val observationEvent = loadSampleData("events/entity/attributeUpdateNumericPropDatasetIdEvent.json")

        coEvery {
            entityPayloadService.partialUpdateAttribute(any(), any(), any())
        } returns UpdateResult(
            emptyList(),
            listOf(NotUpdatedDetails(TEMPERATURE_PROPERTY, "Property does not exist"))
        ).right()

        observationEventListener.dispatchObservationMessage(observationEvent)

        verify { entityEventService wasNot called }
    }

    @Test
    fun `it should parse and transmit an ATTRIBUTE_APPEND event`() = runTest {
        val observationEvent = loadSampleData("events/entity/attributeAppendNumericPropDatasetIdEvent.json")

        coEvery {
            entityPayloadService.appendAttributes(any(), any(), any(), any())
        } returns UpdateResult(
            listOf(
                UpdatedDetails(
                    TEMPERATURE_PROPERTY,
                    expectedTemperatureDatasetId,
                    UpdateOperationResult.APPENDED
                )
            ),
            emptyList()
        ).right()
        val mockedJsonLdEntity = mockkClass(JsonLdEntity::class, relaxed = true)
        every { mockedJsonLdEntity.types } returns listOf(BEEHIVE_TYPE)
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any(), any())
        } returns Job()

        observationEventListener.dispatchObservationMessage(observationEvent)

        coVerify {
            entityPayloadService.appendAttributes(
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
                    it.containsKey(TEMPERATURE_PROPERTY)
                },
                match {
                    it.updated.size == 1 &&
                        it.updated[0].updateOperationResult == UpdateOperationResult.APPENDED &&
                        it.updated[0].attributeName == TEMPERATURE_PROPERTY &&
                        it.updated[0].datasetId == expectedTemperatureDatasetId
                },
                eq(true),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `it should not propagate an ATTRIBUTE_APPEND event if nothing has been appended`() = runTest {
        val observationEvent = loadSampleData("events/entity/attributeAppendNumericPropDatasetIdEvent.json")

        coEvery {
            entityPayloadService.appendAttributes(any(), any(), any(), any())
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

        verify { entityPayloadService wasNot called }
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

        verify { entityPayloadService wasNot called }
        verify { entityEventService wasNot called }
    }
}
