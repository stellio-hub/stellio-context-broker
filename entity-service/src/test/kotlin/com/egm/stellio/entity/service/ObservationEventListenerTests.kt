package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
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
    fun `it should parse and transmit an entity creation event`() {
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
            entityEventService.publishEntityEvent(
                match {
                    it as EntityCreateEvent
                    it.operationType == EventsType.ENTITY_CREATE &&
                        it.entityId == "urn:ngsi-ld:BeeHive:TESTC".toUri() &&
                        it.contexts.contains(APIC_COMPOUND_CONTEXT)
                },
                "https://ontology.eglobalmark.com/apic#BeeHive"
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should parse and transmit an observation event`() {
        val observationEvent = loadSampleData("observations/temperatureUpdateEvent.jsonld")

        every { entityAttributeService.partialUpdateEntityAttribute(any(), any(), any()) } returns true
        every { entityService.getFullEntityById(any(), any()) } returns mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

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
        verify { entityService.getFullEntityById("urn:ngsi-ld:BeeHive:TESTC".toUri(), true) }
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as AttributeUpdateEvent
                    it.entityId == "urn:ngsi-ld:BeeHive:TESTC".toUri() &&
                        it.attributeName == "temperature" &&
                        it.datasetId == "urn:ngsi-ld:Dataset:temperature:1".toUri() &&
                        it.contexts == listOf(APIC_COMPOUND_CONTEXT)
                },
                any()
            )
        }

        confirmVerified()
    }
}
