package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.fail
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
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
        assertEquals(3, teas.size)

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
    fun `it should set a specific access policy for a temporal entity`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.updateSpecificAccessPolicy(
            beehiveTestCId,
            SpecificAccessPolicy.AUTH_READ
        ).shouldSucceed()

        temporalEntityAttributeService.hasSpecificAccessPolicies(
            beehiveTestCId,
            listOf(SpecificAccessPolicy.AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }

        temporalEntityAttributeService.hasSpecificAccessPolicies(
            beehiveTestDId,
            listOf(SpecificAccessPolicy.AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should remove a specific access policy from a temporal entity`() = runTest {
        val rawEntity = loadSampleData()

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.updateSpecificAccessPolicy(beehiveTestCId, SpecificAccessPolicy.AUTH_READ)

        temporalEntityAttributeService.removeSpecificAccessPolicy(beehiveTestCId).shouldSucceed()

        temporalEntityAttributeService.hasSpecificAccessPolicies(
            beehiveTestCId,
            listOf(SpecificAccessPolicy.AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
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
    fun `it should retrieve the temporal attributes of entities`() = runTest {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
            ) { null }

        assertEquals(3, temporalEntityAttributes.size)
        assertThat(temporalEntityAttributes)
            .allMatch {
                it.attributeName in setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            }
    }

    @Test
    fun `it should retrieve the temporal attributes of entities without queryParams attrs`() = runTest {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                QueryParams(
                    offset = 0,
                    limit = 1,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE)
                )
            ) { null }

        assertEquals(3, temporalEntityAttributes.size)
        assertThat(temporalEntityAttributes)
            .allMatch {
                it.attributeName in setOf(
                    INCOMING_PROPERTY,
                    "https://ontology.eglobalmark.com/egm#connectsTo",
                    "https://schema.org/name"
                )
            }
    }

    @Test
    fun `it should retrieve the temporal attributes of entities with respect to limit and offset`() = runTest {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                QueryParams(
                    offset = 0,
                    limit = 1,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                )
            ) { null }

        assertEquals(1, temporalEntityAttributes.size)
    }

    @Test
    fun `it should retrieve the temporal attributes of entities with respect to idPattern`() = runTest {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntities(
                QueryParams(
                    offset = 0,
                    limit = 1,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    idPattern = ".*urn:ngsi-ld:BeeHive:TESTD.*"
                )
            ) { null }

        assertEquals(2, temporalEntityAttributes.size)
        assertThat(temporalEntityAttributes)
            .allMatch {
                it.entityId == beehiveTestDId
            }
    }

    @Test
    fun `it should retrieve the temporal attributes of entities according to access rights`() = runTest {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))

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
                        (temporal_entity_attribute.entity_id IN ('urn:ngsi-ld:BeeHive:TESTD'))
                    )
                """.trimIndent()
            }

        assertEquals(2, temporalEntityAttributes.size)
        assertThat(temporalEntityAttributes)
            .allMatch {
                it.entityId == beehiveTestDId
            }
    }

    @Test
    fun `it should retrieve the temporal attributes of entities according to specific access policy`() = runTest {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.updateSpecificAccessPolicy(beehiveTestCId, SpecificAccessPolicy.AUTH_READ)

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
                        (temporal_entity_attribute.entity_id IN ('urn:ngsi-ld:BeeHive:TESTE'))
                    )
                """.trimIndent()
            }

        assertEquals(1, temporalEntityAttributes.size)
        assertThat(temporalEntityAttributes)
            .allMatch {
                it.entityId == beehiveTestCId
            }
    }

    @Test
    fun `it should retrieve the temporal attributes of entities according to specific access policy and rights`() =
        runTest {
            val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
            val secondRawEntity = loadSampleData("beehive.jsonld")

            coEvery { attributeInstanceService.create(any()) } returns Unit.right()

            temporalEntityAttributeService.createEntityTemporalReferences(
                firstRawEntity,
                listOf(APIC_COMPOUND_CONTEXT)
            )
            temporalEntityAttributeService.createEntityTemporalReferences(
                secondRawEntity,
                listOf(APIC_COMPOUND_CONTEXT)
            )
            temporalEntityAttributeService.updateSpecificAccessPolicy(beehiveTestCId, SpecificAccessPolicy.AUTH_READ)

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
                            (temporal_entity_attribute.entity_id IN ('urn:ngsi-ld:BeeHive:TESTD'))
                        )
                    """.trimIndent()
                }

            assertEquals(3, temporalEntityAttributes.size)
            assertThat(temporalEntityAttributes)
                .allMatch {
                    it.entityId == beehiveTestCId || it.entityId == beehiveTestDId
                }
        }

    @Test
    fun `it should retrieve the count of temporal attributes of entities`() = runTest {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.getCountForEntities(
            QueryParams(
                offset = 0,
                limit = 30,
                ids = setOf(beehiveTestDId, beehiveTestCId),
                types = setOf(BEEHIVE_TYPE),
                attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            )
        ) { null }.shouldSucceedWith { assertEquals(1, it) }
    }

    @Test
    fun `it should retrieve the count of temporal attributes of entities according to access rights`() = runTest {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.getCountForEntities(
            QueryParams(
                offset = 0,
                limit = 30,
                ids = setOf(beehiveTestDId, beehiveTestCId),
                types = setOf(BEEHIVE_TYPE)
            )
        ) { "temporal_entity_attribute.entity_id IN ('urn:ngsi-ld:BeeHive:TESTC')" }
            .shouldSucceedWith { assertEquals(0, it) }

        temporalEntityAttributeService.getCountForEntities(
            QueryParams(
                offset = 0,
                limit = 30,
                ids = setOf(beehiveTestDId, beehiveTestCId),
                types = setOf(BEEHIVE_TYPE)
            )
        ) { "temporal_entity_attribute.entity_id IN ('urn:ngsi-ld:BeeHive:TESTD')" }
            .shouldSucceedWith { assertEquals(1, it) }
    }

    @Test
    fun `it should return an empty list if no temporal attribute matches the requested entities`() = runTest {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        temporalEntityAttributeService.createEntityTemporalReferences(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))

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

        assertThat(temporalEntityAttributes).isEmpty()
    }

    @Test
    fun `it should return an empty list if the requested attributes do not exist in the requested entities`() =
        runTest {
            val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
            val secondRawEntity = loadSampleData("beehive.jsonld")

            coEvery { attributeInstanceService.create(any()) } returns Unit.right()

            temporalEntityAttributeService.createEntityTemporalReferences(
                firstRawEntity,
                listOf(APIC_COMPOUND_CONTEXT)
            )
            temporalEntityAttributeService.createEntityTemporalReferences(
                secondRawEntity,
                listOf(APIC_COMPOUND_CONTEXT)
            )

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

            assertThat(temporalEntityAttributes).isEmpty()
        }

    @Test
    fun `it should delete temporal entity references`() = runTest {
        val entityId = "urn:ngsi-ld:BeeHive:TESTE".toUri()

        coEvery { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(entityId) } returns Unit.right()

        temporalEntityAttributeService.deleteTemporalEntityReferences(entityId).shouldSucceed()
    }

    @Test
    fun `it should delete the two temporal entity attributes`() = runTest {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.deleteTemporalAttributesOfEntity(beehiveTestDId).shouldSucceed()

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestDId, INCOMING_PROPERTY
        ).shouldFail { assertInstanceOf(ResourceNotFoundException::class.java, it) }
    }

    @Test
    fun `it should delete a temporal attribute references`() = runTest {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.deleteTemporalAttributeReferences(
            beehiveTestDId,
            INCOMING_PROPERTY,
            null
        ).shouldSucceed()

        temporalEntityAttributeService.getForEntityAndAttribute(
            beehiveTestDId, INCOMING_PROPERTY
        ).shouldFail { assertInstanceOf(ResourceNotFoundException::class.java, it) }
    }

    @Test
    fun `it should delete references of all temporal attribute instances`() = runTest {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity, listOf(APIC_COMPOUND_CONTEXT))

        temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(
            beehiveTestCId,
            INCOMING_PROPERTY
        ).shouldSucceed()

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
            { assertEquals("Attribute speed was not found", it.message) },
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
