package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
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
                    it.types == listOf(BEEHIVE_TYPE) &&
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
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
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
                        it.time == ZonedDateTime.parse("2020-01-24T13:01:22.066Z") &&
                        it.sub == "0123456789-1234-5678-987654321"
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
    fun `it should set a specific access policy for a temporal entity`() {
        val rawEntity = loadSampleData()

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        StepVerifier
            .create(
                temporalEntityAttributeService.updateSpecificAccessPolicy(
                    beehiveTestCId,
                    SpecificAccessPolicy.AUTH_READ
                )
            )
            .expectNextMatches { it == 3 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(
                temporalEntityAttributeService.hasSpecificAccessPolicies(
                    beehiveTestCId,
                    listOf(SpecificAccessPolicy.AUTH_READ)
                )
            )
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        StepVerifier
            .create(
                temporalEntityAttributeService.hasSpecificAccessPolicies(
                    beehiveTestDId,
                    listOf(SpecificAccessPolicy.AUTH_READ)
                )
            )
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should remove a specific access policy from a temporal entity`() {
        val rawEntity = loadSampleData()

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()
        temporalEntityAttributeService.updateSpecificAccessPolicy(beehiveTestCId, SpecificAccessPolicy.AUTH_READ)
            .block()

        StepVerifier
            .create(temporalEntityAttributeService.removeSpecificAccessPolicy(beehiveTestCId))
            .expectNextMatches { it == 3 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(
                temporalEntityAttributeService.hasSpecificAccessPolicies(
                    beehiveTestCId,
                    listOf(SpecificAccessPolicy.AUTH_READ)
                )
            )
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()
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
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
            ) { null }

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.size == 3 &&
                    it.all { tea ->
                        tea.types == listOf(BEEHIVE_TYPE) &&
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
                QueryParams(
                    offset = 10,
                    limit = 1,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
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
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
            ) {
                """
                    (
                        (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                        OR
                        (entity_id IN ('urn:ngsi-ld:BeeHive:TESTD'))
                    )
                """.trimIndent()
            }

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.size == 2 &&
                    it.all { tea ->
                        tea.types == listOf(BEEHIVE_TYPE) &&
                            tea.entityId == beehiveTestDId
                    }
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve the temporal attributes of entities according to specific access policy`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.updateSpecificAccessPolicy(beehiveTestCId, SpecificAccessPolicy.AUTH_READ)
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
            ) {
                """
                    (
                        (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                        OR
                        (entity_id IN ('urn:ngsi-ld:BeeHive:TESTE'))
                    )
                """.trimIndent()
            }

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.size == 1 &&
                    it[0].types == listOf(BEEHIVE_TYPE) &&
                    it[0].entityId == beehiveTestCId
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve the temporal attributes of entities according to specific access policy and rights`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.updateSpecificAccessPolicy(beehiveTestCId, SpecificAccessPolicy.AUTH_READ)
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
            ) {
                """
                    (
                        (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                        OR
                        (entity_id IN ('urn:ngsi-ld:BeeHive:TESTD'))
                    )
                """.trimIndent()
            }

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.size == 3 &&
                    it.all { tea -> tea.entityId == beehiveTestCId || tea.entityId == beehiveTestDId }
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
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
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
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE)
                )
            ) { "entity_id IN ('urn:ngsi-ld:BeeHive:TESTC')" }

        StepVerifier.create(temporalEntityNoResult)
            .expectNextMatches { it == 0 }
            .verifyComplete()

        val temporalEntityWithResult =
            temporalEntityAttributeService.getCountForEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE)
                )
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
                QueryParams(
                    offset = 10,
                    limit = 2,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf("https://ontology.eglobalmark.com/apic#UnknownType"),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
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
                QueryParams(
                    offset = 10,
                    limit = 2,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf("unknownAttribute")
                )
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

    @Test
    fun `it should return a right unit if entiy and attribute exist`() {
        val rawEntity = loadSampleData()

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        runBlocking {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(beehiveTestCId, INCOMING_PROPERTY).fold(
                { fail("The referred resource should have been found") },
                { }
            )
        }
    }

    @Test
    fun `it should return a left attribute not found if entity exists but not the attribute`() {
        val rawEntity = loadSampleData()

        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.createEntityPayload(any(), any()) } answers { Mono.just(1) }

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        runBlocking {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(beehiveTestCId, "speed").fold(
                { assertEquals("Attribute speed was not found", it.message) },
                { fail("The referred resource should have not been found") }
            )
        }
    }

    @Test
    fun `it should return a left entity not found if entity does not exist`() {
        runBlocking {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(
                "urn:ngsi-ld:Entity:01".toUri(),
                "speed"
            ).fold(
                { assertEquals("Entity urn:ngsi-ld:Entity:01 was not found", it.message) },
                { fail("The referred resource should have not been found") }
            )
        }
    }
}
