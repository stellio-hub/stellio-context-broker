package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
class TemporalEntityAttributeServiceTests : WithTimescaleContainer {

    @Autowired
    @SpykBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var entityPayloadService: EntityPayloadService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    val beehiveTestDId = "urn:ngsi-ld:BeeHive:TESTD".toUri()

    @AfterEach
    fun clearPreviousTemporalEntityAttributesAndObservations() {
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()

        r2dbcEntityTemplate.delete(AttributeInstance::class.java)
            .all()
            .block()

        r2dbcEntityTemplate.delete(TemporalEntityAttribute::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should retrieve a persisted temporal entity attribute`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntity(
                beehiveTestDId,
                setOf(
                    INCOMING_PROPERTY,
                    OUTGOING_PROPERTY
                )
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.entityId == beehiveTestDId &&
                    it.type == BEEHIVE_TYPE &&
                    it.attributeName == INCOMING_PROPERTY
            }
            .expectNextMatches {
                it.entityId == beehiveTestDId
            }
            .expectComplete()
            .verify()

        verify(exactly = 6) { attributeInstanceService.create(any()) }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should create entries for all attributes of an entity`() {
        val rawEntity = loadSampleData()

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT)
        )

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 1
            }
            .expectComplete()
            .verify()

        StepVerifier.create(temporalEntityAttributeService.getForEntity(beehiveTestCId, emptySet()))
            .expectNextCount(3)
            .expectComplete()
            .verify()

        verify {
            entityPayloadService.createEntityPayload(beehiveTestCId, any())
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1543.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2020-01-24T13:01:22.066Z")
                }
            )
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1543.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time == ZonedDateTime.parse("2020-01-24T14:01:22.066Z")
                }
            )
            attributeInstanceService.create(
                match {
                    it.value == "ParisBeehive12" &&
                        it.measuredValue == null &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2020-01-24T13:01:22.066Z")
                }
            )
            attributeInstanceService.create(
                match {
                    it.value == "urn:ngsi-ld:Beekeeper:Pascal" &&
                        it.measuredValue == null &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2010-10-26T21:32:52.986010Z")
                }
            )
        }
        confirmVerified(entityPayloadService, attributeInstanceService)
    }

    @Test
    fun `it should create entries for a multi-instance property`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT)
        )

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 1
            }
            .expectComplete()
            .verify()

        StepVerifier.create(temporalEntityAttributeService.getForEntity(beehiveTestCId, emptySet()))
            .expectNextCount(2)
            .expectComplete()
            .verify()

        verify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1543.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2020-01-24T13:01:21.938Z")
                }
            )
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1543.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time == ZonedDateTime.parse("2020-01-24T14:01:22.066Z")
                }
            )
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1618.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2020-01-24T13:01:21.942Z")
                }
            )
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1618.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time == ZonedDateTime.parse("2020-01-24T14:08:22.066Z")
                }
            )
        }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should return the temporalEntityAttributeId of a given entityId and attributeName`() {
        val rawEntity = loadSampleData()

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should return the temporalEntityAttributeId of a given entityId attributeName and datasetId`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY,
            "urn:ngsi-ld:Dataset:01234".toUri()
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not return a temporalEntityAttributeId if the datasetId is unknown`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY,
            "urn:ngsi-ld:Dataset:Unknown".toUri()
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve the temporal attributes of entities`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                10,
                0,
                setOf(beehiveTestDId, beehiveTestCId),
                setOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            ) { null }

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.size == 3 &&
                    it.all { tea ->
                        tea.type == BEEHIVE_TYPE &&
                            tea.attributeName in setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                    }
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve the temporal attributes of entities with respect to limit and offset`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(2) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                10,
                1,
                setOf(beehiveTestDId, beehiveTestCId),
                setOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            ) { null }

        StepVerifier.create(temporalEntityAttributes)
            .expectNextCount(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve the temporal attributes of entities according to access rights`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                10,
                0,
                setOf(beehiveTestDId, beehiveTestCId),
                setOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            ) { "entity_id IN ('urn:ngsi-ld:BeeHive:TESTD')" }

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.size == 2 &&
                    it.all { tea ->
                        tea.type == BEEHIVE_TYPE &&
                            tea.entityId == beehiveTestDId
                    }
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve the count of temporal attributes of entities`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntity =
            temporalEntityAttributeService.getCountForEntities(
                setOf(beehiveTestDId, beehiveTestCId),
                setOf(BEEHIVE_TYPE),
                setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            ) { null }

        StepVerifier.create(temporalEntity)
            .expectNextMatches { it == 1 }
            .verifyComplete()
    }

    @Test
    fun `it should retrieve the count of temporal attributes of entities according to access rights`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityNoResult =
            temporalEntityAttributeService.getCountForEntities(
                setOf(beehiveTestDId, beehiveTestCId),
                setOf(BEEHIVE_TYPE),
                emptySet()
            ) { "entity_id IN ('urn:ngsi-ld:BeeHive:TESTC')" }

        StepVerifier.create(temporalEntityNoResult)
            .expectNextMatches { it == 0 }
            .verifyComplete()

        val temporalEntityWithResult =
            temporalEntityAttributeService.getCountForEntities(
                setOf(beehiveTestDId, beehiveTestCId),
                setOf(BEEHIVE_TYPE),
                emptySet()
            ) { "entity_id IN ('urn:ngsi-ld:BeeHive:TESTD')" }

        StepVerifier.create(temporalEntityWithResult)
            .expectNextMatches { it == 1 }
            .verifyComplete()
    }

    @Test
    fun `it should return an empty list if no temporal attribute matches the requested entities`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                10,
                2,
                setOf(beehiveTestDId, beehiveTestCId),
                setOf("https://ontology.eglobalmark.com/apic#UnknownType"),
                setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            ) { null }

        StepVerifier.create(temporalEntityAttributes)
            .expectNext(emptyList())
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should return an empty list if the requested attributes do not exist in the requested entities`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                10,
                2,
                setOf(beehiveTestDId, beehiveTestCId),
                setOf(BEEHIVE_TYPE),
                setOf("unknownAttribute")
            ) { null }

        StepVerifier.create(temporalEntityAttributes)
            .expectNext(emptyList())
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete temporal entity references`() {
        val entityId = "urn:ngsi-ld:BeeHive:TESTE".toUri()

        every { entityPayloadService.deleteEntityPayload(entityId) } answers { Mono.just(1) }
        every { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(entityId) } answers { Mono.just(2) }

        val deletedRecords = temporalEntityAttributeService.deleteTemporalEntityReferences(entityId).block()

        assertEquals(2, deletedRecords)
    }

    @Test
    fun `it should delete the two temporal entity attributes`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val deletedRecords = temporalEntityAttributeService.deleteTemporalAttributesOfEntity(beehiveTestDId).block()

        assertEquals(4, deletedRecords)

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestDId, INCOMING_PROPERTY
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete a temporal attribute references`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val deletedRecords = temporalEntityAttributeService.deleteTemporalAttributeReferences(
            beehiveTestDId,
            INCOMING_PROPERTY,
            null
        ).block()

        assertEquals(1, deletedRecords)

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestDId, INCOMING_PROPERTY
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete references of all temporal attribute instances`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val deletedRecords = temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).block()

        assertEquals(2, deletedRecords)

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId, INCOMING_PROPERTY
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }
}
