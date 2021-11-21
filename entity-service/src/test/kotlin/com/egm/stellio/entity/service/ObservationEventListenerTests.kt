package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Runs
import io.mockk.called
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.mockkClass
import io.mockk.verify
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

    @Test
    fun `it should parse and transmit an ENTITY_CREATE event`() {
        val observationEvent = loadSampleData("observations/beehiveCreateEvent.jsonld")

        observationEventListener.processMessage(observationEvent)

        verify {
            entityService.createEntity(
                match {
                    it.id == "urn:ngsi-ld:BeeHive:TESTC".toUri() &&
                        it.type == "https://ontology.eglobalmark.com/apic#BeeHive" &&
                        it.relationships.size == 1 &&
                        it.relationships[0].compactName == "belongs" &&
                        it.relationships[0].name == "https://ontology.eglobalmark.com/egm#belongs" &&
                        it.relationships[0].instances.size == 1 &&
                        it.relationships[0].instances[0].objectId == "urn:ngsi-ld:Apiary:XYZ01".toUri()
                }
            )
        }

        verify {
            entityEventService.publishEntityCreateEvent(
                eq("urn:ngsi-ld:BeeHive:TESTC".toUri()),
                eq("https://ontology.eglobalmark.com/apic#BeeHive"),
                any(),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should parse and transmit an ATTRIBUTE_UPDATE event`() {
        val observationEvent = loadSampleData("observations/temperatureUpdateEvent.jsonld")

        every { entityAttributeService.partialUpdateEntityAttribute(any(), any(), any()) } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    "https://ontology.eglobalmark.com/apic#temperature",
                    "urn:ngsi-ld:Dataset:temperature:1".toUri(),
                    UpdateOperationResult.UPDATED
                )
            ),
            notUpdated = arrayListOf()
        )

        every { entityEventService.publishPartialUpdateEntityAttributesEvents(any(), any(), any(), any()) } just Runs

        observationEventListener.processMessage(observationEvent)

        verify {
            entityAttributeService.partialUpdateEntityAttribute(
                "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                match {
                    it.containsKey("https://ontology.eglobalmark.com/apic#temperature")
                },
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
        verify {
            entityEventService.publishPartialUpdateEntityAttributesEvents(
                eq("urn:ngsi-ld:BeeHive:TESTC".toUri()),
                match { it.containsKey("temperature") },
                match {
                    it.size == 1 &&
                        it[0].attributeName == "temperature" &&
                        it[0].datasetId == "urn:ngsi-ld:Dataset:temperature:1".toUri() &&
                        it[0].updateOperationResult == UpdateOperationResult.UPDATED
                },
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should parse and transmit an ATTRIBUTE_APPEND event`() {
        val observationEvent = loadSampleData("observations/humidityAppendEvent.jsonld")

        every { entityService.appendEntityAttributes(any(), any(), any()) } returns UpdateResult(
            listOf(
                UpdatedDetails(
                    "https://ontology.eglobalmark.com/apic#humidity",
                    "urn:ngsi-ld:Dataset:humidity:1".toUri(),
                    UpdateOperationResult.APPENDED
                )
            ),
            emptyList()
        )
        val mockedJsonLdEntity = mockkClass(JsonLdEntity::class, relaxed = true)
        every { mockedJsonLdEntity.type } returns "https://ontology.eglobalmark.com/apic#BeeHive"
        every { entityEventService.publishAttributeAppendEvent(any(), any()) } just Runs

        observationEventListener.processMessage(observationEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                match {
                    it.size == 1 &&
                        it.first().compactName == "humidity" &&
                        it.first().getAttributeInstances().size == 1
                },
                true
            )
        }
        verify {
            entityEventService.publishAttributeAppendEvent(
                match {
                    it.entityId == "urn:ngsi-ld:BeeHive:TESTC".toUri() &&
                        it.attributeName == "humidity" &&
                        it.datasetId == "urn:ngsi-ld:Dataset:humidity:1".toUri() &&
                        it.contexts == listOf(APIC_COMPOUND_CONTEXT) && !it.overwrite
                },
                eq(UpdateOperationResult.APPENDED)
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should ignore an ATTRIBUTE_APPEND events for unparsable attributes`() {
        val observationEvent = loadSampleData("observations/unparseable/humidityAppendEvent.jsonld")

        observationEventListener.processMessage(observationEvent)

        verify { entityService wasNot called }
        verify { entityEventService wasNot called }

        confirmVerified(entityService, entityEventService)
    }
}
