package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
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
    private lateinit var entityAttributeService: EntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    private val expectedEntityId = "urn:ngsi-ld:BeeHive:01".toUri()
    private val expectedTemperatureDatasetId = "urn:ngsi-ld:Dataset:WeatherApi".toUri()

    @Test
    fun `it should parse and transmit an ENTITY_CREATE event`() {
        val observationEvent = loadSampleData("events/entity/entityCreateEvent.json")

        observationEventListener.processMessage(observationEvent)

        verify {
            entityService.createEntity(
                match {
                    it.id == expectedEntityId &&
                        it.type == BEEHIVE_TYPE &&
                        it.relationships.size == 2 &&
                        it.properties.size == 2 &&
                        it.geoProperties.size == 1
                }
            )
        }

        verify {
            entityEventService.publishEntityCreateEvent(
                eq("0123456789-1234-5678-987654321"),
                eq(expectedEntityId),
                eq(BEEHIVE_TYPE),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should parse and transmit an ATTRIBUTE_UPDATE event`() {
        val observationEvent = loadSampleData("events/entity/attributeUpdateNumericPropDatasetIdEvent.json")

        every { entityAttributeService.partialUpdateEntityAttribute(any(), any(), any()) } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    TEMPERATURE_PROPERTY,
                    expectedTemperatureDatasetId,
                    UpdateOperationResult.UPDATED
                )
            ),
            notUpdated = arrayListOf()
        )

        every { entityEventService.publishPartialAttributeUpdateEvents(any(), any(), any(), any(), any()) } just Runs

        observationEventListener.processMessage(observationEvent)

        verify {
            entityAttributeService.partialUpdateEntityAttribute(
                expectedEntityId,
                match { it.containsKey(TEMPERATURE_PROPERTY) },
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
        verify {
            entityEventService.publishPartialAttributeUpdateEvents(
                null,
                eq(expectedEntityId),
                match { it.containsKey(TEMPERATURE_PROPERTY) },
                match {
                    it.size == 1 &&
                        it[0].attributeName == TEMPERATURE_PROPERTY &&
                        it[0].datasetId == expectedTemperatureDatasetId &&
                        it[0].updateOperationResult == UpdateOperationResult.UPDATED
                },
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should parse and transmit an ATTRIBUTE_APPEND event`() {
        val observationEvent = loadSampleData("events/entity/attributeAppendNumericPropDatasetIdEvent.json")

        every { entityService.appendEntityAttributes(any(), any(), any()) } returns UpdateResult(
            listOf(
                UpdatedDetails(
                    TEMPERATURE_PROPERTY,
                    expectedTemperatureDatasetId,
                    UpdateOperationResult.APPENDED
                )
            ),
            emptyList()
        )
        val mockedJsonLdEntity = mockkClass(JsonLdEntity::class, relaxed = true)
        every { mockedJsonLdEntity.type } returns BEEHIVE_TYPE
        every {
            entityEventService.publishAttributeAppendEvent(
                any(), any(), any(), any(), any(), any(), any(), any()
            )
        } just Runs

        observationEventListener.processMessage(observationEvent)

        verify {
            entityService.appendEntityAttributes(
                expectedEntityId,
                match {
                    it.size == 1 &&
                        it.first().compactName == TEMPERATURE_COMPACT_PROPERTY &&
                        it.first().getAttributeInstances().size == 1
                },
                false
            )
        }
        verify {
            entityEventService.publishAttributeAppendEvent(
                null,
                eq(expectedEntityId),
                eq(TEMPERATURE_COMPACT_PROPERTY),
                eq(expectedTemperatureDatasetId),
                eq(true),
                any(),
                eq(UpdateOperationResult.APPENDED),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should ignore an invalid ATTRIBUTE_APPEND event`() {
        val observationEvent = loadSampleData("events/entity/invalid/humidityAppendEvent.jsonld")

        observationEventListener.processMessage(observationEvent)

        verify { entityService wasNot called }
        verify { entityEventService wasNot called }

        confirmVerified(entityService, entityEventService)
    }
}
