package com.egm.stellio.search.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityEventListenerService::class])
@ActiveProfiles("test")
class EntityEventListenerServiceTest {

    @Autowired
    private lateinit var entityEventListenerService: EntityEventListenerService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @MockkBean
    private lateinit var entityPayloadService: EntityPayloadService

    private val expectedEntityId = "urn:ngsi-ld:BeeHive:01"

    @Test
    fun `it should return an invalid result if the attribute has an unsupported type`() {
        val compactedJsonLdEntity =
            mapOf(
                "type" to "GeoProperty",
                "observedAt" to "2021-04-12T09:00:00Z",
                "value" to mapOf(
                    "type" to "Point",
                    "coordinates" to listOf(12.1234, 21.4321)
                )
            )
        val result = entityEventListenerService.toTemporalAttributeMetadata(compactedJsonLdEntity)
        result.bimap(
            {
                assertEquals(
                    "Unsupported attribute type: GeoProperty",
                    it
                )
            },
            {
                fail<String>("Expecting an invalid result, got a valid one: $it")
            }
        )
    }

    @Test
    fun `it should return an invalid result if the attribute has no value`() {
        val compactedJsonLdEntity =
            mapOf(
                "type" to "Property",
                "observedAt" to "2021-04-12T09:00:00Z"
            )
        val result = entityEventListenerService.toTemporalAttributeMetadata(compactedJsonLdEntity)
        result.bimap(
            {
                assertEquals(
                    "Unable to get a value from attribute: {type=Property, observedAt=2021-04-12T09:00:00Z}",
                    it
                )
            },
            {
                fail<String>("Expecting an invalid result, got a valid one: $it")
            }
        )
    }

    @Test
    fun `it should return attribute metadata if the payload is valid`() {
        val compactedJsonLdEntity =
            mapOf(
                "type" to "Relationship",
                "object" to "urn:ngsi-ld:Entity:1234",
                "createdAt" to "2021-01-02T19:00:00.123Z",
                "observedAt" to "2021-04-12T09:00:00.123Z",
                "datasetId" to "urn:ngsi-ld:Dataset:1234"
            )
        val result = entityEventListenerService.toTemporalAttributeMetadata(compactedJsonLdEntity)
        result.bimap(
            {
                fail<String>("Expecting a valid result, got an invalid one: $it")
            },
            {
                assertEquals(TemporalEntityAttribute.AttributeType.Relationship, it.type)
                assertEquals("urn:ngsi-ld:Entity:1234", it.value)
                assertEquals(TemporalEntityAttribute.AttributeValueType.ANY, it.valueType)
                assertEquals("urn:ngsi-ld:Dataset:1234", it.datasetId.toString())
                assertEquals("2021-01-02T19:00:00.123Z", it.createdAt.toString())
                assertEquals("2021-04-12T09:00:00.123Z", it.observedAt.toString())
            }
        )
    }

    @Test
    fun `it should handle an ENTITY_CREATE event`() = runTest {
        val entityCreateEventPayload = loadSampleData("events/entity/entityCreateEvent.json")

        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any<String>(), any(), any())
        } returns Unit.right()
        coEvery { entityAccessRightsService.setAdminRoleOnEntity(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(entityCreateEventPayload)

        coVerify {
            temporalEntityAttributeService.createEntityTemporalReferences(
                match {
                    it.contains(expectedEntityId)
                },
                listOf(APIC_COMPOUND_CONTEXT),
                eq("0123456789-1234-5678-987654321")
            )
            entityAccessRightsService.setAdminRoleOnEntity(
                eq("0123456789-1234-5678-987654321"),
                eq(expectedEntityId.toUri())
            )
        }
        confirmVerified(temporalEntityAttributeService, entityAccessRightsService)
    }

    @Test
    fun `it should handle an ENTITY_REPLACE event`() = runTest {
        val entityCreateEventPayload = loadSampleData("events/entity/entityReplaceEvent.json")

        coEvery { temporalEntityAttributeService.deleteTemporalEntityReferences(any()) } returns Unit.right()
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any<String>(), any(), any())
        } returns Unit.right()
        coEvery { entityAccessRightsService.setAdminRoleOnEntity(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(entityCreateEventPayload)

        coVerify {
            temporalEntityAttributeService.deleteTemporalEntityReferences(expectedEntityId.toUri())
            temporalEntityAttributeService.createEntityTemporalReferences(
                match {
                    it.contains(expectedEntityId)
                },
                listOf(APIC_COMPOUND_CONTEXT)
            )
            entityAccessRightsService.setAdminRoleOnEntity(null, expectedEntityId.toUri())
        }
        confirmVerified(temporalEntityAttributeService, entityAccessRightsService)
    }

    @Test
    fun `it should handle an ENTITY_DELETE event`() = runTest {
        val entityDeleteEventPayload = loadSampleData("events/entity/entityDeleteEvent.json")

        coEvery { temporalEntityAttributeService.deleteTemporalEntityReferences(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.removeRolesOnEntity(any()) } returns Unit.right()

        entityEventListenerService.processMessage(entityDeleteEventPayload)

        coVerify {
            temporalEntityAttributeService.deleteTemporalEntityReferences(eq(expectedEntityId.toUri()))
            entityAccessRightsService.removeRolesOnEntity(eq(expectedEntityId.toUri()))
        }
        confirmVerified(temporalEntityAttributeService, entityAccessRightsService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_DELETE event`() = runTest {
        val attributeDeleteEventPayload = loadSampleData("events/entity/attributeDeleteEvent.json")

        coEvery {
            temporalEntityAttributeService.deleteTemporalAttributeReferences(any(), any(), any())
        } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeDeleteEventPayload)

        coVerify {
            temporalEntityAttributeService.deleteTemporalAttributeReferences(
                eq(expectedEntityId.toUri()),
                eq(NAME_PROPERTY),
                null
            )
            entityPayloadService.upsertEntityPayload(
                eq(expectedEntityId.toUri()),
                match { it.contains(expectedEntityId) }
            )
        }
        confirmVerified(temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_DELETE_ALL_INSTANCES event`() = runTest {
        val attributeDeleteAllInstancesEvent = loadSampleData("events/entity/attributeDeleteAllInstancesEvent.json")

        coEvery {
            temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(any(), any())
        } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeDeleteAllInstancesEvent)

        coVerify {
            temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(
                eq(expectedEntityId.toUri()),
                eq(TEMPERATURE_PROPERTY)
            )
            entityPayloadService.upsertEntityPayload(
                eq(expectedEntityId.toUri()),
                match { it.contains(expectedEntityId) }
            )
        }
        confirmVerified(temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_APPEND event for a text property`() = runTest {
        val attributeAppendEventPayload = loadSampleData("events/entity/attributeAppendTextPropEvent.json")

        coEvery { temporalEntityAttributeService.create(any()) } returns Unit.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        coVerify {
            temporalEntityAttributeService.create(
                match {
                    it.entityId == expectedEntityId.toUri() &&
                        it.types == listOf(BEEHIVE_TYPE) &&
                        it.attributeName == NAME_PROPERTY &&
                        it.attributeType == TemporalEntityAttribute.AttributeType.Property &&
                        it.attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY &&
                        it.datasetId == null
                }
            )
            attributeInstanceService.create(
                match {
                    it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-12T08:40:29.239195Z") &&
                        it.value == "BeeHiveSophia" &&
                        it.measuredValue == null &&
                        it.payload.contains("createdAt")
                }
            )
            entityPayloadService.upsertEntityPayload(
                eq(expectedEntityId.toUri()),
                match { it.contains(expectedEntityId) }
            )
        }
        confirmVerified(attributeInstanceService, temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_APPEND event for a numeric property having an observed date`() = runTest {
        val attributeAppendEventPayload = loadSampleData("events/entity/attributeAppendNumericPropEvent.json")

        coEvery { temporalEntityAttributeService.create(any()) } returns Unit.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        coVerify {
            temporalEntityAttributeService.create(
                match {
                    it.entityId == expectedEntityId.toUri() &&
                        it.types == listOf(BEEHIVE_TYPE) &&
                        it.attributeName == LUMINOSITY_PROPERTY &&
                        it.attributeType == TemporalEntityAttribute.AttributeType.Property &&
                        it.attributeValueType == TemporalEntityAttribute.AttributeValueType.MEASURE &&
                        it.datasetId == null
                }
            )
            attributeInstanceService.create(
                match {
                    it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-13T08:02:56.987108Z") &&
                        it.value == null &&
                        it.measuredValue == 120.0 &&
                        it.payload.contains("createdAt")
                }
            )
            attributeInstanceService.create(
                match {
                    it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-02T21:32:52.986010Z") &&
                        it.value == null &&
                        it.measuredValue == 120.0 &&
                        it.payload.contains("createdAt")
                }
            )
            entityPayloadService.upsertEntityPayload(
                eq(expectedEntityId.toUri()),
                match { it.contains(expectedEntityId) }
            )
        }

        confirmVerified(attributeInstanceService, temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_APPEND event for a numeric property having dataset id`() = runTest {
        val attributeAppendEventPayload =
            loadSampleData("events/entity/attributeAppendNumericPropDatasetIdEvent.json")

        coEvery { temporalEntityAttributeService.create(any()) } returns Unit.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        coVerify {
            temporalEntityAttributeService.create(
                match {
                    it.entityId == expectedEntityId.toUri() &&
                        it.types == listOf(BEEHIVE_TYPE) &&
                        it.attributeName == TEMPERATURE_PROPERTY &&
                        it.attributeType == TemporalEntityAttribute.AttributeType.Property &&
                        it.attributeValueType == TemporalEntityAttribute.AttributeValueType.MEASURE &&
                        it.datasetId == "urn:ngsi-ld:Dataset:WeatherApi".toUri()
                }
            )
            attributeInstanceService.create(
                match {
                    it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-13T14:05:02.239954Z") &&
                        it.value == null &&
                        it.measuredValue == 30.0 &&
                        it.payload.contains("createdAt")
                }
            )
            attributeInstanceService.create(
                match {
                    it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time == ZonedDateTime.parse("2021-10-26T21:32:52.986010Z") &&
                        it.value == null &&
                        it.measuredValue == 30.0 &&
                        it.payload.contains("createdAt")
                }
            )
            entityPayloadService.upsertEntityPayload(
                eq(expectedEntityId.toUri()),
                match { it.contains(expectedEntityId) }
            )
        }
        confirmVerified(attributeInstanceService, temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_APPEND event for a relationship`() = runTest {
        val attributeAppendEventPayload = loadSampleData("events/entity/attributeAppendRelEvent.json")

        coEvery { temporalEntityAttributeService.create(any()) } returns Unit.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        coVerify {
            temporalEntityAttributeService.create(
                match {
                    it.entityId == expectedEntityId.toUri() &&
                        it.types == listOf(BEEHIVE_TYPE) &&
                        it.attributeName == CREATED_BY_RELATIONSHIP &&
                        it.attributeType == TemporalEntityAttribute.AttributeType.Relationship &&
                        it.attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY &&
                        it.datasetId == null
                }
            )
            attributeInstanceService.create(
                match {
                    it.timeProperty == AttributeInstance.TemporalProperty.CREATED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-13T08:07:08.429628Z") &&
                        it.value == "urn:ngsi-ld:Craftman:01" &&
                        it.measuredValue == null &&
                        it.payload.contains("createdAt")
                }
            )
            entityPayloadService.upsertEntityPayload(
                eq(expectedEntityId.toUri()),
                match { it.contains(expectedEntityId) }
            )
        }
        confirmVerified(attributeInstanceService, temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_APPEND event for a type`() = runTest {
        val attributeAppendEventPayload = loadSampleData("events/entity/attributeAppendTypeEvent.json")

        coEvery { temporalEntityAttributeService.updateTemporalEntityTypes(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        coVerify {
            temporalEntityAttributeService.updateTemporalEntityTypes(
                eq(expectedEntityId.toUri()),
                match {
                    it.containsAll(listOf(BEEHIVE_TYPE, APIARY_TYPE))
                }
            )
            entityPayloadService.upsertEntityPayload(
                eq(expectedEntityId.toUri()),
                match { it.contains(expectedEntityId) }
            )
        }
        confirmVerified(temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_REPLACE event for an observed numeric property`() = runTest {
        val attributeReplaceEventPayload = loadSampleData("events/entity/attributeReplaceNumericPropEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeReplaceEventPayload)

        verifyMocksForAttributeUpdateOrReplace(
            TEMPERATURE_PROPERTY
        ) {
            attributeInstanceService.create(
                match {
                    it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time == ZonedDateTime.parse("2021-11-26T22:35:52.98601Z") &&
                        it.value == null &&
                        it.measuredValue == 44.0 &&
                        it.payload.contains("createdAt")
                }
            )
        }
    }

    @Test
    fun `it should handle an ATTRIBUTE_REPLACE event for an observed numeric property not existing previously`() =
        runTest {
            val attributeReplaceEventPayload = loadSampleData("events/entity/attributeReplaceNumericPropEvent.json")

            coEvery {
                temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
            } returns ResourceNotFoundException("Attribute does not exist").left()
            coEvery { temporalEntityAttributeService.create(any()) } returns Unit.right()
            coEvery { attributeInstanceService.create(any()) } returns Unit.right()
            coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

            entityEventListenerService.processMessage(attributeReplaceEventPayload)

            coVerify {
                temporalEntityAttributeService.create(
                    match {
                        it.entityId == expectedEntityId.toUri() &&
                            it.types == listOf(BEEHIVE_TYPE) &&
                            it.attributeName == TEMPERATURE_PROPERTY &&
                            it.attributeType == TemporalEntityAttribute.AttributeType.Property &&
                            it.attributeValueType == TemporalEntityAttribute.AttributeValueType.MEASURE &&
                            it.datasetId == null
                    }
                )
            }
            verifyMocksForAttributeUpdateOrReplace(
                TEMPERATURE_PROPERTY
            ) {
                attributeInstanceService.create(
                    match {
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                            it.time == ZonedDateTime.parse("2021-11-26T22:35:52.98601Z") &&
                            it.value == null &&
                            it.measuredValue == 44.0 &&
                            it.payload.contains("createdAt")
                    }
                )
            }
        }

    @Test
    fun `it should handle an ATTRIBUTE_REPLACE event for a text property`() = runTest {
        val attributeReplaceEventPayload = loadSampleData("events/entity/attributeReplaceTextPropEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeReplaceEventPayload)

        verifyMocksForAttributeUpdateOrReplace(
            NAME_PROPERTY
        ) {
            attributeInstanceService.create(
                match {
                    it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.timeProperty == AttributeInstance.TemporalProperty.MODIFIED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-13T14:14:03.758379Z") &&
                        it.value == "Beehive - Biot" &&
                        it.measuredValue == null &&
                        it.payload.contains("createdAt")
                }
            )
        }
    }

    @Test
    fun `it should handle an ATTRIBUTE_REPLACE event for a relationship`() = runTest {
        val attributeReplaceEventPayload = loadSampleData("events/entity/attributeReplaceRelEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeReplaceEventPayload)

        verifyMocksForAttributeUpdateOrReplace(
            MANAGED_BY_RELATIONSHIP
        ) {
            attributeInstanceService.create(
                match {
                    it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.timeProperty == AttributeInstance.TemporalProperty.MODIFIED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-13T14:00:45.114035Z") &&
                        it.value == "urn:ngsi-ld:Beekeeper:02" &&
                        it.measuredValue == null &&
                        it.payload.contains("createdAt")
                }
            )
        }
    }

    @Test
    fun `it should not update entity payload if the creation of the attribute instance has failed`() = runTest {
        val attributeUpdateEventPayload = loadSampleData("events/entity/attributeUpdateRelEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns InternalErrorException("DB is down").left()

        entityEventListenerService.processMessage(attributeUpdateEventPayload)

        coVerify { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) }
        coVerify { entityPayloadService.upsertEntityPayload(any(), any()) wasNot Called }
        confirmVerified(temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_UPDATE event for a numeric property having an observed date`() = runTest {
        val attributeUpdateEventPayload = loadSampleData("events/entity/attributeUpdateNumericPropEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeUpdateEventPayload)

        verifyMocksForAttributeUpdateOrReplace(
            TEMPERATURE_PROPERTY
        ) {
            attributeInstanceService.create(
                match {
                    it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time == ZonedDateTime.parse("2021-12-08T10:20:30.986010Z") &&
                        it.value == null &&
                        it.measuredValue == 2.0 &&
                        it.payload.contains("createdAt")
                }
            )
        }
    }

    @Test
    fun `it should handle an ATTRIBUTE_UPDATE event for a numeric property having a dataset id`() = runTest {
        val attributeUpdateEventPayload =
            loadSampleData("events/entity/attributeUpdateNumericPropDatasetIdEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any(), any())
        } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeUpdateEventPayload)

        verifyMocksForAttributeUpdateOrReplace(
            TEMPERATURE_PROPERTY,
            "urn:ngsi-ld:Dataset:WeatherApi".toUri()
        ) {
            attributeInstanceService.create(
                match {
                    it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                        it.time == ZonedDateTime.parse("2021-12-08T10:20:30.986010Z") &&
                        it.value == null &&
                        it.measuredValue == 2.0 &&
                        it.payload.contains("createdAt")
                }
            )
        }
    }

    @Test
    fun `it should handle an ATTRIBUTE_UPDATE event for a relationship`() = runTest {
        val attributeUpdateEventPayload = loadSampleData("events/entity/attributeUpdateRelEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeUpdateEventPayload)

        verifyMocksForAttributeUpdateOrReplace(
            MANAGED_BY_RELATIONSHIP
        ) {
            attributeInstanceService.create(
                match {
                    it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.timeProperty == AttributeInstance.TemporalProperty.MODIFIED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-13T14:10:16.963869Z") &&
                        it.value == "urn:ngsi-ld:Beekeeper:02" &&
                        it.measuredValue == null &&
                        it.payload.contains("createdAt")
                }
            )
        }
    }

    @Test
    fun `it should handle an ATTRIBUTE_UPDATE event for a text property`() = runTest {
        val attributeUpdateEventPayload = loadSampleData("events/entity/attributeUpdateTextPropEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } returns temporalEntityAttributeUuid.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertEntityPayload(any(), any()) } returns Unit.right()

        entityEventListenerService.processMessage(attributeUpdateEventPayload)

        verifyMocksForAttributeUpdateOrReplace(
            NAME_PROPERTY,
        ) {
            attributeInstanceService.create(
                match {
                    it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        it.timeProperty == AttributeInstance.TemporalProperty.MODIFIED_AT &&
                        it.time == ZonedDateTime.parse("2022-02-13T14:12:42.361048Z") &&
                        it.value == "Beehive - Valbonne" &&
                        it.measuredValue == null &&
                        it.payload.contains("createdAt")
                }
            )
        }
    }

    private fun verifyMocksForAttributeUpdateOrReplace(
        attributeName: ExpandedTerm,
        datasetId: URI? = null,
        attributeInstanceVerifyBlock: suspend MockKVerificationScope.() -> Unit
    ) {
        coVerify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(expectedEntityId.toUri()),
                eq(attributeName),
                matchNullable { expected -> expected == datasetId }
            )
        }
        coVerify(verifyBlock = attributeInstanceVerifyBlock)

        coVerify {
            entityPayloadService.upsertEntityPayload(
                expectedEntityId.toUri(),
                match {
                    it.contains(expectedEntityId)
                }
            )
        }

        confirmVerified(attributeInstanceService, temporalEntityAttributeService, entityPayloadService)
    }
}
