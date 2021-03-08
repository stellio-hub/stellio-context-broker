package com.egm.stellio.search.service

import com.egm.stellio.search.config.TimescaleBasedTests
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
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
class TemporalEntityAttributeServiceTests : TimescaleBasedTests() {

    @Autowired
    @SpykBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

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
        databaseClient.delete()
            .from("entity_payload")
            .fetch()
            .rowsUpdated()
            .block()

        databaseClient.delete()
            .from("attribute_instance")
            .fetch()
            .rowsUpdated()
            .block()

        databaseClient.delete()
            .from("temporal_entity_attribute")
            .fetch()
            .rowsUpdated()
            .block()
    }

    @Test
    fun `it should retrieve a persisted temporal entity attribute`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(apicContext!!)).block()

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
            listOf(apicContext!!)
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
            listOf(apicContext!!)
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
            listOf(apicContext!!)
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
            listOf(apicContext!!)
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

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(apicContext!!)).block()

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

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(apicContext!!)).block()

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

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(apicContext!!)).block()

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

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(apicContext!!)).block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(apicContext!!)).block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                setOf("urn:ngsi-ld:BeeHive:TESTD".toUri(), "urn:ngsi-ld:BeeHive:TESTC".toUri()),
                setOf("https://ontology.eglobalmark.com/apic#BeeHive"),
                setOf(
                    incomingAttrExpandedName,
                    outgoingAttrExpandedName
                )
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.keys.first() == "urn:ngsi-ld:BeeHive:TESTD".toUri() &&
                    it.values.first().size == 2 &&
                    it.values.first().all {
                        it.type == "https://ontology.eglobalmark.com/apic#BeeHive" &&
                            it.attributeName in setOf(incomingAttrExpandedName, outgoingAttrExpandedName)
                    }
            }
            .expectNextMatches {
                it.keys.first() == "urn:ngsi-ld:BeeHive:TESTC".toUri() &&
                    it.values.first().size == 1 &&
                    it.values.first().all {
                        it.type == "https://ontology.eglobalmark.com/apic#BeeHive" &&
                            it.attributeName in setOf(incomingAttrExpandedName)
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

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(apicContext!!)).block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(apicContext!!)).block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                setOf("urn:ngsi-ld:BeeHive:TESTD".toUri(), "urn:ngsi-ld:BeeHive:TESTC".toUri()),
                setOf("https://ontology.eglobalmark.com/apic#UnknownType"),
                setOf(
                    incomingAttrExpandedName,
                    outgoingAttrExpandedName
                )
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should return an empty list if the requested attributes does not exist in the requested entities`() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        every { attributeInstanceService.create(any()) } returns Mono.just(1)

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(apicContext!!)).block()
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(apicContext!!)).block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                setOf("urn:ngsi-ld:BeeHive:TESTD".toUri(), "urn:ngsi-ld:BeeHive:TESTC".toUri()),
                setOf("https://ontology.eglobalmark.com/apic#BeeHive"),
                setOf("unknownAttribute")
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }
}
