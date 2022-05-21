package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

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
    fun `it should handle an ENTITY_CREATE event`() {
        val entityCreateEventPayload = loadSampleData("events/entity/entityCreateEvent.json")

        every {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any())
        } answers { Mono.just(1) }
        every { entityAccessRightsService.setAdminRoleOnEntity(any(), any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(entityCreateEventPayload)

        verify {
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
    fun `it should handle an ENTITY_REPLACE event`() {
        val entityCreateEventPayload = loadSampleData("events/entity/entityReplaceEvent.json")

        every { temporalEntityAttributeService.deleteTemporalEntityReferences(any()) } answers { Mono.just(1) }
        every {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any())
        } answers { Mono.just(1) }
        every { entityAccessRightsService.setAdminRoleOnEntity(any(), any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(entityCreateEventPayload)

        verify {
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
    fun `it should handle an ENTITY_DELETE event`() {
        val entityDeleteEventPayload = loadSampleData("events/entity/entityDeleteEvent.json")

        every { temporalEntityAttributeService.deleteTemporalEntityReferences(any()) } answers { Mono.just(10) }
        every { entityAccessRightsService.removeRolesOnEntity(any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(entityDeleteEventPayload)

        verify {
            temporalEntityAttributeService.deleteTemporalEntityReferences(eq(expectedEntityId.toUri()))
            entityAccessRightsService.removeRolesOnEntity(eq(expectedEntityId.toUri()))
        }
        confirmVerified(temporalEntityAttributeService, entityAccessRightsService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_DELETE event`() {
        val attributeDeleteEventPayload = loadSampleData("events/entity/attributeDeleteEvent.json")

        every {
            temporalEntityAttributeService.deleteTemporalAttributeReferences(any(), any(), any())
        } answers { Mono.just(4) }
        every { entityPayloadService.upsertEntityPayload(any(), any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(attributeDeleteEventPayload)

        verify {
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
    fun `it should handle an ATTRIBUTE_DELETE_ALL_INSTANCES event`() {
        val attributeDeleteAllInstancesEvent = loadSampleData("events/entity/attributeDeleteAllInstancesEvent.json")

        every {
            temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(any(), any())
        } answers { Mono.just(8) }
        every { entityPayloadService.upsertEntityPayload(any(), any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(attributeDeleteAllInstancesEvent)

        verify {
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
    fun `it should handle an ATTRIBUTE_APPEND event for a text property`() {
        val attributeAppendEventPayload = loadSampleData("events/entity/attributeAppendTextPropEvent.json")

        every { temporalEntityAttributeService.create(any()) } answers { Mono.just(1) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.upsertEntityPayload(any(), any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        verify {
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
    fun `it should handle an ATTRIBUTE_APPEND event for a numeric property having an observed date`() {
        val attributeAppendEventPayload = loadSampleData("events/entity/attributeAppendNumericPropEvent.json")

        every { temporalEntityAttributeService.create(any()) } answers { Mono.just(1) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.upsertEntityPayload(any(), any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        verify {
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
    fun `it should handle an ATTRIBUTE_APPEND event for a numeric property having dataset id`() {
        val attributeAppendEventPayload =
            loadSampleData("events/entity/attributeAppendNumericPropDatasetIdEvent.json")

        every { temporalEntityAttributeService.create(any()) } answers { Mono.just(1) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.upsertEntityPayload(any(), any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        verify {
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
    fun `it should handle an ATTRIBUTE_APPEND event for a relationship`() {
        val attributeAppendEventPayload = loadSampleData("events/entity/attributeAppendRelEvent.json")

        every { temporalEntityAttributeService.create(any()) } answers { Mono.just(1) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }
        every { entityPayloadService.upsertEntityPayload(any(), any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(attributeAppendEventPayload)

        verify {
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
    fun `it should handle an ATTRIBUTE_REPLACE event for an observed numeric property`() {
        val attributeReplaceEventPayload = loadSampleData("events/entity/attributeReplaceNumericPropEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

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
    fun `it should handle an ATTRIBUTE_REPLACE event for an observed numeric property not existing previously`() {
        val attributeReplaceEventPayload = loadSampleData("events/entity/attributeReplaceNumericPropEvent.json")

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.empty() }
        every { temporalEntityAttributeService.create(any()) } answers { Mono.just(1) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

        entityEventListenerService.processMessage(attributeReplaceEventPayload)

        verify {
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
    fun `it should handle an ATTRIBUTE_REPLACE event for a text property`() {
        val attributeReplaceEventPayload = loadSampleData("events/entity/attributeReplaceTextPropEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

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
    fun `it should handle an ATTRIBUTE_REPLACE event for a relationship`() {
        val attributeReplaceEventPayload = loadSampleData("events/entity/attributeReplaceRelEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

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
    fun `it should not update entity payload if the creation of the attribute instance has failed`() {
        val attributeUpdateEventPayload = loadSampleData("events/entity/attributeUpdateRelEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.error(RuntimeException()) }

        entityEventListenerService.processMessage(attributeUpdateEventPayload)

        verify { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) }
        verify { entityPayloadService.upsertEntityPayload(any(), any()) wasNot Called }
        confirmVerified(temporalEntityAttributeService, entityPayloadService)
    }

    @Test
    fun `it should handle an ATTRIBUTE_UPDATE event for a numeric property having an observed date`() {
        val attributeUpdateEventPayload = loadSampleData("events/entity/attributeUpdateNumericPropEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

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
    fun `it should handle an ATTRIBUTE_UPDATE event for a numeric property having a dataset id`() {
        val attributeUpdateEventPayload =
            loadSampleData("events/entity/attributeUpdateNumericPropDatasetIdEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any(), any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

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
    fun `it should handle an ATTRIBUTE_UPDATE event for a relationship`() {
        val attributeUpdateEventPayload = loadSampleData("events/entity/attributeUpdateRelEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

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
    fun `it should handle an ATTRIBUTE_UPDATE event for a text property`() {
        val attributeUpdateEventPayload = loadSampleData("events/entity/attributeUpdateTextPropEvent.json")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.just(temporalEntityAttributeUuid) }
        every { attributeInstanceService.create(any()) } answers { Mono.just(1) }

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
        attributeInstanceVerifyBlock: MockKVerificationScope.() -> Unit
    ) {
        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(expectedEntityId.toUri()),
                eq(attributeName),
                matchNullable { expected -> expected == datasetId }
            )
        }
        verify(verifyBlock = attributeInstanceVerifyBlock)

        verify {
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
