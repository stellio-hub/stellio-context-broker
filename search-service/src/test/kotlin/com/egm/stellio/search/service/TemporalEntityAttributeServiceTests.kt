package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.toNgsiLdAttribute
import com.egm.stellio.shared.model.toNgsiLdAttributes
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.coEvery
import io.mockk.coVerify
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
                entityId = beehiveTestCId,
                types = listOf(BEEHIVE_TYPE),
                createdAt = Instant.now().atZone(UTC),
                contexts = listOf(APIC_COMPOUND_CONTEXT),
                payload = EMPTY_JSON_PAYLOAD
            )
        ).block()

        r2dbcEntityTemplate.insert<EntityPayload>().into("entity_payload").using(
            EntityPayload(
                entityId = beehiveTestDId,
                types = listOf(BEEHIVE_TYPE),
                createdAt = Instant.now().atZone(UTC),
                contexts = listOf(APIC_COMPOUND_CONTEXT),
                payload = EMPTY_JSON_PAYLOAD
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
                        it.time.isAfter(ngsiLdDateTime().minusMinutes(1)) &&
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
                        it.time.isAfter(ngsiLdDateTime().minusMinutes(1))
                }
            )
            attributeInstanceService.create(
                match {
                    it.value == "urn:ngsi-ld:Beekeeper:Pascal" &&
                        it.measuredValue == null &&
                        it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time.isAfter(ngsiLdDateTime().minusMinutes(1))
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
                        it.time.isAfter(ngsiLdDateTime().minusMinutes(1))
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
                        it.time.isAfter(ngsiLdDateTime().minusMinutes(1))
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

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceed()

        val temporalEntityAttribute = temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).shouldSucceedAndResult()

        val createdAt = ngsiLdDateTime()
        val newProperty = loadSampleData("fragments/beehive_new_incoming_property.json")
        val expandedAttribute = expandAttribute(newProperty, listOf(APIC_COMPOUND_CONTEXT))
        val newNgsiLdProperty = expandedAttribute.toNgsiLdAttribute().shouldSucceedAndResult()
        temporalEntityAttributeService.replaceAttribute(
            temporalEntityAttribute,
            newNgsiLdProperty,
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
            expandedAttribute.second[0],
            null
        ).shouldSucceed()

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).shouldSucceedWith {
            assertEquals(TemporalEntityAttribute.AttributeType.Property, it.attributeType)
            assertEquals(TemporalEntityAttribute.AttributeValueType.STRING, it.attributeValueType)
            assertEquals(beehiveTestCId, it.entityId)
            assertJsonPayloadsAreEqual(serializeObject(expandedAttribute.second[0]), it.payload.asString())
            assertTrue(it.createdAt.isBefore(createdAt))
            assertEquals(createdAt, it.modifiedAt)
        }
    }

    @Test
    fun `it should merge an entity attribute`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceed()

        val temporalEntityAttribute = temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).shouldSucceedAndResult()

        val mergedAt = ngsiLdDateTime()
        val propertyToMerge = loadSampleData("fragments/beehive_mergeAttribute.json")
        val expandedAttribute = expandAttribute(propertyToMerge, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.mergeAttribute(
            temporalEntityAttribute,
            INCOMING_PROPERTY,
            AttributeMetadata(
                null,
                "It's a string now",
                null,
                TemporalEntityAttribute.AttributeValueType.STRING,
                null,
                TemporalEntityAttribute.AttributeType.Property,
                ZonedDateTime.parse("2022-12-24T14:01:22.066Z")
            ),
            mergedAt,
            null,
            expandedAttribute.second[0],
            null
        ).shouldSucceed()

        val expectedMergedPayload = expandAttribute(
            """
                {
                    "incoming": {
                        "type": "Property",
                        "value": "It's a string now",
                        "observedAt": "2022-12-24T14:01:22.066Z",
                        "observedBy": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:Sensor:IncomingSensor"
                        }
                    }
                }
            """.trimIndent(),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).shouldSucceedWith {
            assertEquals(TemporalEntityAttribute.AttributeType.Property, it.attributeType)
            assertEquals(TemporalEntityAttribute.AttributeValueType.STRING, it.attributeValueType)
            assertJsonPayloadsAreEqual(serializeObject(expectedMergedPayload.second[0]), it.payload.asString())
            assertTrue(it.createdAt.isBefore(mergedAt))
            assertEquals(mergedAt, it.modifiedAt)
        }
    }

    @Test
    fun `it should merge an entity attributes`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceed()

        val createdAt = ngsiLdDateTime()
        val attributesToMerge = loadSampleData("fragments/beehive_mergeAttributes.json")
        val expandedAttributes = JsonLdUtils.expandAttributes(attributesToMerge, listOf(APIC_COMPOUND_CONTEXT))
        val ngsiLdAttributes = expandedAttributes.toMap().toNgsiLdAttributes().shouldSucceedAndResult()
        temporalEntityAttributeService.mergeEntityAttributes(
            beehiveTestCId,
            ngsiLdAttributes,
            expandedAttributes,
            createdAt,
            null,
            null
        ).shouldSucceedWith { updateResult ->
            val updatedDetails = updateResult.updated
            assertEquals(6, updatedDetails.size)
            assertEquals(4, updatedDetails.filter { it.updateOperationResult == UpdateOperationResult.UPDATED }.size)
            assertEquals(2, updatedDetails.filter { it.updateOperationResult == UpdateOperationResult.APPENDED }.size)
            val newAttributes = updatedDetails.filter { it.updateOperationResult == UpdateOperationResult.APPENDED }
                .map { it.attributeName }
            assertTrue(newAttributes.containsAll(listOf(OUTGOING_PROPERTY, TEMPERATURE_PROPERTY)))
        }

        val teas = temporalEntityAttributeService.getForEntity(
            beehiveTestCId,
            emptySet()
        )
        assertEquals(6, teas.size)
        coVerify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1575.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time.isEqual(ZonedDateTime.parse("2022-01-24T14:01:22.066Z"))
                }
            )
        }
        // 1 when we create entity attributes and 3 when we merge observed entity attributes
        coVerify(exactly = 4) {
            attributeInstanceService.create(
                match { it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT }
            )
        }
        // 0 when we create entity attributes and 3 when we merge non-observed entity attributes
        coVerify(exactly = 3) {
            attributeInstanceService.create(
                match { it.timeProperty == AttributeInstance.TemporalProperty.MODIFIED_AT }
            )
        }
        // 4 when we create entity attributes and 2 when we append new entity attributes
        coVerify(exactly = 6) {
            attributeInstanceService.create(
                match { it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT }
            )
        }
    }

    @Test
    fun `it should merge an entity attributes with observedAt parameter`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceed()

        val createdAt = ngsiLdDateTime()
        val observedAt = ZonedDateTime.parse("2019-12-04T12:00:00.00Z")
        val propertyToMerge = loadSampleData("fragments/beehive_mergeAttribute_without_observedAt.json")
        val expandedAttributes = JsonLdUtils.expandAttributes(propertyToMerge, listOf(APIC_COMPOUND_CONTEXT))
        val ngsiLdAttributes = expandedAttributes.toMap().toNgsiLdAttributes().shouldSucceedAndResult()
        temporalEntityAttributeService.mergeEntityAttributes(
            beehiveTestCId,
            ngsiLdAttributes,
            expandedAttributes,
            createdAt,
            observedAt,
            null
        ).shouldSucceedWith { updateResult ->
            val updatedDetails = updateResult.updated
            assertEquals(1, updatedDetails.size)
            assertEquals(1, updatedDetails.filter { it.updateOperationResult == UpdateOperationResult.UPDATED }.size)
        }

        coVerify(exactly = 1) {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 1560.0 &&
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time.isEqual(ZonedDateTime.parse("2019-12-04T12:00:00.00Z"))
                }
            )
        }
    }

    @Test
    fun `it should replace an entity attribute`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceed()

        val replacedAt = ngsiLdDateTime()
        val propertyToReplace = loadSampleData("fragments/beehive_new_incoming_property.json")
        val expandedAttribute = expandAttribute(propertyToReplace, listOf(APIC_COMPOUND_CONTEXT))
        val ngsiLdAttribute = expandedAttribute.toNgsiLdAttribute().shouldSucceedAndResult()

        temporalEntityAttributeService.replaceEntityAttribute(
            beehiveTestCId,
            ngsiLdAttribute,
            expandedAttribute,
            replacedAt,
            null
        ).shouldSucceed()

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).shouldSucceedWith {
            assertEquals(TemporalEntityAttribute.AttributeType.Property, it.attributeType)
            assertEquals(TemporalEntityAttribute.AttributeValueType.STRING, it.attributeValueType)
            assertJsonPayloadsAreEqual(serializeObject(expandedAttribute.second[0]), it.payload.asString())
            assertTrue(it.createdAt.isBefore(replacedAt))
            assertEquals(replacedAt, it.modifiedAt)
        }
    }

    @Test
    fun `it should ignore a replace attribute if attribute does not exist`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceed()

        val replacedAt = ngsiLdDateTime()
        val propertyToReplace = loadSampleData("fragments/beehive_new_unknown_property.json")
        val expandedAttribute = expandAttribute(propertyToReplace, listOf(APIC_COMPOUND_CONTEXT))
        val ngsiLdAttribute = expandedAttribute.toNgsiLdAttribute().shouldSucceedAndResult()

        temporalEntityAttributeService.replaceEntityAttribute(
            beehiveTestCId,
            ngsiLdAttribute,
            expandedAttribute,
            replacedAt,
            null
        ).shouldSucceedWith {
            assertTrue(it.updated.isEmpty())
            assertEquals(1, it.notUpdated.size)
            val notUpdatedDetails = it.notUpdated.first()
            assertEquals(NGSILD_DEFAULT_VOCAB + "unknown", notUpdatedDetails.attributeName)
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
