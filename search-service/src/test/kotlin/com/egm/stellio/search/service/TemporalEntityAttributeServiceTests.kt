package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.fail
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class TemporalEntityAttributeServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    @SpykBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    val beehiveTestDId = "urn:ngsi-ld:BeeHive:TESTD".toUri()

    @BeforeEach
    fun bootstrapEntities() {
        r2dbcEntityTemplate.insert<EntityPayload>().into("entity_payload").using(
            EntityPayload(
                beehiveTestCId,
                listOf(BEEHIVE_TYPE),
                Instant.now().atZone(UTC),
                null,
                listOf(APIC_COMPOUND_CONTEXT),
                EMPTY_JSON_PAYLOAD
            )
        ).block()

        r2dbcEntityTemplate.insert<EntityPayload>().into("entity_payload").using(
            EntityPayload(
                beehiveTestDId,
                listOf(BEEHIVE_TYPE),
                Instant.now().atZone(UTC),
                null,
                listOf(APIC_COMPOUND_CONTEXT),
                EMPTY_JSON_PAYLOAD
            )
        ).block()
    }

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
    fun `it should retrieve a persisted temporal entity attribute`() = runTest {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntity(
                beehiveTestDId,
                setOf(
                    INCOMING_PROPERTY,
                    OUTGOING_PROPERTY
                )
            )

        assertEquals(2, temporalEntityAttributes.size)
        assertTrue(listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY).contains(temporalEntityAttributes[0].attributeName))

        coVerify(exactly = 6) { attributeInstanceService.create(any()) }
    }

    @Test
    fun `it should create entries for all attributes of an entity`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val teas = temporalEntityAttributeService.getForEntity(beehiveTestCId, emptySet())
        assertEquals(4, teas.size)

        coVerify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1543.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time.isAfter(ZonedDateTime.now().minusMinutes(1)) &&
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
                        it.time.isAfter(ZonedDateTime.now().minusMinutes(1))
                }
            )
            attributeInstanceService.create(
                match {
                    it.value == "urn:ngsi-ld:Beekeeper:Pascal" &&
                        it.measuredValue == null &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time.isAfter(ZonedDateTime.now().minusMinutes(1))
                }
            )
        }
    }

    @Test
    fun `it should create entries for a multi-instance property`() = runTest {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT)
        ).shouldSucceed()

        val teas = temporalEntityAttributeService.getForEntity(beehiveTestCId, emptySet())
        assertEquals(2, teas.size)

        coVerify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1543.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time.isAfter(ZonedDateTime.now().minusMinutes(1))
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
                        it.time.isAfter(ZonedDateTime.now().minusMinutes(1))
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
    }

    @Test
    fun `it should rollback the whole operation if one DB update fails`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery {
            attributeInstanceService.create(
                match { it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT }
            )
        } throws RuntimeException("Unexpected DB error!")

        assertThrows<RuntimeException>("it should have thrown a RuntimeException") {
            temporalEntityAttributeService.createEntityTemporalReferences(
                rawEntity,
                listOf(APIC_COMPOUND_CONTEXT)
            ).shouldSucceed()
        }

        val teas = temporalEntityAttributeService.getForEntity("urn:ngsi-ld:BeeHive:TESTC".toUri(), emptySet())
        assertTrue(teas.isEmpty())
    }

    @Test
    fun `it should replace a temporal entity attribute`() = runTest {
        val rawEntity = loadSampleData()

        val temporalEntityAttribute = mockkClass(TemporalEntityAttribute::class) {
            every { id } returns UUID.randomUUID()
            every { entityId } returns beehiveTestCId
        }
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceed()

        val createdAt = ZonedDateTime.now()
        val newProperty = loadSampleData("fragments/beehive_new_incoming_property.json")
        val jsonLdAttribute = JsonLdUtils.expandJsonLdFragment(newProperty, listOf(APIC_COMPOUND_CONTEXT))
        val newNgsiLdProperty = parseToNgsiLdAttributes(jsonLdAttribute.toMap())
        temporalEntityAttributeService.replaceAttribute(
            temporalEntityAttribute,
            newNgsiLdProperty[0],
            AttributeMetadata(
                null,
                "It's a string now",
                null,
                TemporalEntityAttribute.AttributeValueType.STRING,
                null,
                TemporalEntityAttribute.AttributeType.Property,
                ZonedDateTime.parse("2022-12-24T14:01:22.066Z")
            ),
            createdAt,
            jsonLdAttribute,
            null
        )

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).shouldSucceedWith {
            it.attributeType == TemporalEntityAttribute.AttributeType.Property &&
                it.attributeValueType == TemporalEntityAttribute.AttributeValueType.STRING &&
                it.entityId == beehiveTestCId &&
                it.payload.asString() == serializeObject(jsonLdAttribute) &&
                it.createdAt.isBefore(createdAt) &&
                it.modifiedAt == createdAt
        }
    }

    @Test
    fun `it should return the temporalEntityAttributeId of a given entityId and attributeName`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).shouldSucceedWith { assertNotNull(it) }
    }

    @Test
    fun `it should return the temporalEntityAttributeId of a given entityId attributeName and datasetId`() = runTest {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY,
            "urn:ngsi-ld:Dataset:01234".toUri()
        ).shouldSucceedWith { assertNotNull(it) }
    }

    @Test
    fun `it should not return a temporalEntityAttributeId if the datasetId is unknown`() = runTest {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY,
            "urn:ngsi-ld:Dataset:Unknown".toUri()
        ).shouldFail {
            assertInstanceOf(ResourceNotFoundException::class.java, it)
        }
    }

    @Test
    fun `it should delete a temporal attribute references`() = runTest {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { attributeInstanceService.deleteInstancesOfAttribute(any(), any(), any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.deleteTemporalAttribute(
            beehiveTestDId,
            INCOMING_PROPERTY,
            null
        ).shouldSucceed()

        coVerify {
            attributeInstanceService.deleteInstancesOfAttribute(eq(beehiveTestDId), eq(INCOMING_PROPERTY), null)
        }

        temporalEntityAttributeService.getForEntityAndAttribute(beehiveTestDId, INCOMING_PROPERTY)
            .shouldFail { assertInstanceOf(ResourceNotFoundException::class.java, it) }
    }

    @Test
    fun `it should delete references of all temporal attribute instances`() = runTest {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { attributeInstanceService.deleteAllInstancesOfAttribute(any(), any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.deleteTemporalAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY,
            null,
            deleteAll = true
        ).shouldSucceed()

        coVerify {
            attributeInstanceService.deleteAllInstancesOfAttribute(eq(beehiveTestCId), eq(INCOMING_PROPERTY))
        }

        temporalEntityAttributeService.getForEntityAndAttribute(beehiveTestCId, INCOMING_PROPERTY)
            .shouldFail { assertInstanceOf(ResourceNotFoundException::class.java, it) }
    }

    @Test
    fun `it should return a right unit if entiy and attribute exist`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.checkEntityAndAttributeExistence(beehiveTestCId, INCOMING_PROPERTY)
            .shouldSucceed()
    }

    @Test
    fun `it should return a left attribute not found if entity exists but not the attribute`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        val result = temporalEntityAttributeService.checkEntityAndAttributeExistence(beehiveTestCId, "speed")

        result.fold(
            { assertEquals("Attribute speed (default datasetId) was not found", it.message) },
            { fail("The referred resource should have not been found") }
        )
    }

    @Test
    fun `it should return a left entity not found if entity does not exist`() = runTest {
        temporalEntityAttributeService.checkEntityAndAttributeExistence(
            "urn:ngsi-ld:Entity:01".toUri(),
            "speed"
        ).fold(
            { assertEquals("Entity urn:ngsi-ld:Entity:01 was not found", it.message) },
            { fail("The referred resource should have not been found") }
        )
    }
}
