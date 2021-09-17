package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
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

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    val incomingAttrExpandedName = "https://ontology.eglobalmark.com/apic#incoming"
    val outgoingAttrExpandedName = "https://ontology.eglobalmark.com/apic#outgoing"

    val incomingAttributeInstance =
        """
    {   
        "type": "Property",
        "value": 1543,
        "observedAt": "2020-01-24T13:01:22.066Z",
        "observedBy": {
          "type": "Relationship",
          "object": "urn:ngsi-ld:Sensor:IncomingSensor"
        }
    }
        """.trimIndent()

    val outgoingAttributeInstance =
        """
    {    
        "type": "Property",
        "value": 666,
        "observedAt": "2020-01-25T17:01:22.066Z",
        "observedBy": {
          "type": "Relationship",
          "object": "urn:ngsi-ld:Sensor:OutgoingSensor"
        }
    }
        """.trimIndent()

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

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntity(
                "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                setOf(
                    incomingAttrExpandedName,
                    outgoingAttrExpandedName
                )
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.entityId == "urn:ngsi-ld:BeeHive:TESTD".toUri() &&
                    it.type == "https://ontology.eglobalmark.com/apic#BeeHive" &&
                    it.attributeName == incomingAttrExpandedName
            }
            .expectNextMatches {
                it.entityId == "urn:ngsi-ld:BeeHive:TESTD".toUri()
            }
            .expectComplete()
            .verify()

        verify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue in listOf(1543.0, 666.0) &&
                        it.observedAt in listOf(
                        ZonedDateTime.parse("2020-01-24T13:01:22.066Z"),
                        ZonedDateTime.parse("2020-01-25T17:01:22.066Z")
                    )
                }
            )
        }
    }

    @Test
    fun `it should create one entry for an entity with one temporal property`() {
        val rawEntity = loadSampleData()

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT)
        )

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 2
            }
            .expectComplete()
            .verify()

        verify {
            temporalEntityAttributeService.createEntityPayload("urn:ngsi-ld:BeeHive:TESTC".toUri(), any())
        }

        verify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1543.0 &&
                        it.observedAt == ZonedDateTime.parse("2020-01-24T13:01:22.066Z")
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should create two entries for an entity with a two instances property`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT)
        )

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 3
            }
            .expectComplete()
            .verify()

        verify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1543.0 &&
                        it.observedAt == ZonedDateTime.parse("2020-01-24T13:01:22.066Z")
                }
            )
        }
    }

    @Test
    fun `it should create two entries for an entity with two temporal properties`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT)
        )

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 3
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should create two entries for an entity with two attribute instances with payload`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT)
        ).block()

        verify {
            attributeInstanceService.create(
                match {
                    val payload = serializeObject(
                        deserializeObject(it.payload).filterKeys { it != "instanceId" }
                    )
                    it.value == null &&
                        it.measuredValue in listOf(1543.0, 666.0) &&
                        it.observedAt in listOf(
                        ZonedDateTime.parse("2020-01-24T13:01:22.066Z"),
                        ZonedDateTime.parse("2020-01-25T17:01:22.066Z")
                    ) &&
                        (
                            payload.matchContent(incomingAttributeInstance) ||
                                payload.matchContent(outgoingAttributeInstance)
                            )
                }
            )
        }
    }

    @Test
    fun `it should return the temporalEntityAttributeId of a given entityId and attributeName`() {
        val rawEntity = loadSampleData()

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(), incomingAttrExpandedName
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should return the temporalEntityAttributeId of a given entityId attributeName and datasetId`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            incomingAttrExpandedName, "urn:ngsi-ld:Dataset:01234".toUri()
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not return a temporalEntityAttributeId if the datasetId is unknown`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            incomingAttrExpandedName, "urn:ngsi-ld:Dataset:Unknown".toUri()
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve the persisted temporal attributes of the requested entities`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                10,
                2,
                setOf("urn:ngsi-ld:BeeHive:TESTD".toUri(), "urn:ngsi-ld:BeeHive:TESTC".toUri()),
                setOf("https://ontology.eglobalmark.com/apic#BeeHive"),
                setOf(
                    incomingAttrExpandedName,
                    outgoingAttrExpandedName
                )
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.size == 3 &&
                    it.all { tea ->
                        tea.type == "https://ontology.eglobalmark.com/apic#BeeHive" &&
                            tea.attributeName in setOf(incomingAttrExpandedName, outgoingAttrExpandedName)
                    }
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should return an empty list no temporal attribute matches the requested entities`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                10,
                2,
                setOf("urn:ngsi-ld:BeeHive:TESTD".toUri(), "urn:ngsi-ld:BeeHive:TESTC".toUri()),
                setOf("https://ontology.eglobalmark.com/apic#UnknownType"),
                setOf(
                    incomingAttrExpandedName,
                    outgoingAttrExpandedName
                )
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNext(emptyList())
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should return an empty list if the requested attributes does not exist in the requested entities`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                10,
                2,
                setOf("urn:ngsi-ld:BeeHive:TESTD".toUri(), "urn:ngsi-ld:BeeHive:TESTC".toUri()),
                setOf("https://ontology.eglobalmark.com/apic#BeeHive"),
                setOf("unknownAttribute")
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNext(emptyList())
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete temporal entity references`() {
        val entityId = "urn:ngsi-ld:BeeHive:TESTE".toUri()

        every { temporalEntityAttributeService.deleteEntityPayload(entityId) } returns Mono.just(1)
        every { attributeInstanceService.deleteAttributeInstancesOfEntity(entityId) } returns Mono.just(2)
        every { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(entityId) } returns Mono.just(2)

        val deletedRecords = temporalEntityAttributeService.deleteTemporalEntityReferences(entityId).block()

        assert(deletedRecords == 2)

        verify {
            attributeInstanceService.deleteAttributeInstancesOfEntity(entityId)
        }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should delete the two temporal entity attributes`() {
        val entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri()
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        val deletedRecords = temporalEntityAttributeService.deleteTemporalAttributesOfEntity(entityId).block()

        assert(deletedRecords == 2)

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            entityId, incomingAttrExpandedName
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete a temporal attribute references`() {
        val entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri()
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        every {
            attributeInstanceService.deleteAttributeInstancesOfTemporalAttribute(any(), any(), any())
        } returns Mono.just(1)

        val deletedRecords = temporalEntityAttributeService.deleteTemporalAttributeReferences(
            entityId,
            incomingAttrExpandedName,
            null
        ).block()

        assert(deletedRecords == 2)

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            entityId, incomingAttrExpandedName
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete references of all temporal attribute instances`() {
        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT)).block()

        every {
            attributeInstanceService.deleteAllAttributeInstancesOfTemporalAttribute(any(), any())
        } returns Mono.just(2)

        val deletedRecords = temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(
            entityId,
            incomingAttrExpandedName
        ).block()

        assert(deletedRecords == 4)

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            entityId, incomingAttrExpandedName
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }
}
