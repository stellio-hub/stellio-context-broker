package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.parseAndExpandAttributeFragment
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.concurrent.SettableListenableFuture

@SpringBootTest(classes = [EntityEventService::class])
@ActiveProfiles("test")
class EntityEventServiceTests {

    @Autowired
    private lateinit var entityEventService: EntityEventService

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    private val breedingServiceUri = "urn:ngsi-ld:BreedingService:0214".toUri()
    private val breedingServiceType = "https://ontology.eglobalmark.com/aquac#Specie"
    private val fishNameAttribute = "https://ontology.eglobalmark.com/aquac#fishName"
    private val fishNumberAttribute = "https://ontology.eglobalmark.com/aquac#fishNumber"
    private val fishName1DatasetUri = "urn:ngsi-ld:Dataset:fishName:1".toUri()
    private val fishName2DatasetUri = "urn:ngsi-ld:Dataset:fishName:2".toUri()

    @Test
    fun `it should not validate a topic name with characters not supported by Kafka`() {
        assertTrue(
            entityEventService
                .composeTopicName("https://some.host/type")
                .isInvalid
        )
    }

    @Test
    fun `it should validate a topic name with characters supported by Kafka`() {
        assertTrue(
            entityEventService
                .composeTopicName("Specie")
                .isValid
        )
    }

    @Test
    fun `it should publish an ENTITY_CREATE event`() {
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        entityEventService.publishEntityCreateEvent(
            breedingServiceUri, breedingServiceType, "operationPayload", listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify { kafkaTemplate.send("cim.entity.Specie", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should not publish an event if topic name is invalid`() {
        val result =
            entityEventService.publishEntityCreateEvent(
                breedingServiceUri,
                "https://some.host/type",
                "operationPayload",
                listOf(AQUAC_COMPOUND_CONTEXT)
            )

        assertFalse(result)
        verify { kafkaTemplate.send(any(), any(), any()) wasNot Called }
    }

    @Test
    fun `it should publish an ATTRIBUTE_APPEND event if an attribute was appended`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val fishNumberAttributeFragment =
            """
            {
                "fishNumber": {
                    "type": "Property",
                    "value": 120
                }
            }            
            """.trimIndent()
        val jsonLdAttributes = expandJsonLdFragment(fishNumberAttributeFragment, listOf(AQUAC_COMPOUND_CONTEXT))
        val appendResult = UpdateResult(
            listOf(UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.APPENDED)),
            emptyList()
        )

        entityEventService.publishAppendEntityAttributesEvents(
            breedingServiceUri,
            jsonLdAttributes,
            appendResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> {
                    it as AttributeAppendEvent
                    it.operationType == EventsType.ATTRIBUTE_APPEND &&
                        it.entityId == breedingServiceUri &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(fishNumberAttributeFragment) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_APPEND and ATTRIBUTE_REPLACE events if attributes were appended and replaced`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val fishNumberAttributeFragment =
            """
            "fishNumber": {
                "type": "Property",
                "value": 120
            }
            """.trimIndent()
        val fishNameAttributeFragment =
            """
            "fishName": {
                "type": "Property",
                "datasetId": "$fishName1DatasetUri",
                "value": 50
            }
            """.trimIndent()
        val attributesFragment = "{ $fishNumberAttributeFragment, $fishNameAttributeFragment }"
        val jsonLdAttributes = expandJsonLdFragment(attributesFragment, listOf(AQUAC_COMPOUND_CONTEXT))
        val appendResult = UpdateResult(
            listOf(
                UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.APPENDED),
                UpdatedDetails(fishNameAttribute, fishName1DatasetUri, UpdateOperationResult.REPLACED)
            ),
            emptyList()
        )

        entityEventService.publishAppendEntityAttributesEvents(
            breedingServiceUri,
            jsonLdAttributes,
            appendResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> { entityEvent ->
                    when (entityEvent) {
                        is AttributeAppendEvent -> {
                            listOf(entityEvent).any {
                                it.entityId == breedingServiceUri &&
                                    it.attributeName == "fishNumber" &&
                                    it.datasetId == null &&
                                    it.operationPayload.matchContent("{$fishNumberAttributeFragment}") &&
                                    it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                            }
                        }
                        is AttributeReplaceEvent -> {
                            listOf(entityEvent).any {
                                it.entityId == breedingServiceUri &&
                                    it.attributeName == "fishName" &&
                                    it.datasetId == fishName1DatasetUri &&
                                    it.operationPayload.matchContent("{$fishNameAttributeFragment}") &&
                                    it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                            }
                        }
                        else -> false
                    }
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_REPLACE events if two attributes are replaced`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val fishNumberPayload =
            """
            "fishNumber":{
                "type":"Property",
                "value":600
            }
            """.trimIndent()
        val fishNamePayload =
            """
            "fishName":{
                "type":"Property",
                "datasetId": "$fishName1DatasetUri",
                "value":"Salmon",
                "unitCode": "C1"
            }
            """.trimIndent()
        val attributePayload = "{ $fishNumberPayload, $fishNamePayload }"
        val jsonLdAttributes = expandJsonLdFragment(attributePayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNameAttribute, fishName1DatasetUri, UpdateOperationResult.REPLACED),
                UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf()
        )

        entityEventService.publishAppendEntityAttributesEvents(
            breedingServiceUri,
            jsonLdAttributes,
            updateResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it as AttributeReplaceEvent
                        it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                            it.entityId == breedingServiceUri &&
                            (it.attributeName == "fishName" || it.attributeName == "fishNumber") &&
                            (it.datasetId == fishName1DatasetUri || it.datasetId == null) &&
                            (
                                "{$fishNumberPayload}".matchContent(it.operationPayload) ||
                                    "{$fishNamePayload}".matchContent(it.operationPayload)
                                ) &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_REPLACE events if a multi-attribute is replaced`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val fishNamePayload1 =
            """
            {
                "type":"Property",
                "datasetId":"urn:ngsi-ld:Dataset:fishName:1",
                "value":"Salmon",
                "unitCode":"C1"
            }
            """.trimIndent()
        val fishNamePayload2 =
            """
            {
                "type":"Property",
                "datasetId":"urn:ngsi-ld:Dataset:fishName:2",
                "value":"Salmon2",
                "unitCode":"C1"
            }            
            """.trimIndent()
        val attributePayload = "{ \"fishName\": [$fishNamePayload1,$fishNamePayload2] }"
        val jsonLdAttributes = expandJsonLdFragment(attributePayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNameAttribute, fishName1DatasetUri, UpdateOperationResult.REPLACED),
                UpdatedDetails(fishNameAttribute, fishName2DatasetUri, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf()
        )

        entityEventService.publishAppendEntityAttributesEvents(
            breedingServiceUri,
            jsonLdAttributes,
            updateResult,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it as AttributeReplaceEvent
                        it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                            it.entityId == breedingServiceUri &&
                            it.attributeName == "fishName" &&
                            (
                                it.datasetId == fishName1DatasetUri || it.datasetId == fishName2DatasetUri
                                ) &&
                            (
                                it.operationPayload.matchContent("{\"fishName\": $fishNamePayload1}") ||
                                    it.operationPayload.matchContent("{\"fishName\": $fishNamePayload2}")
                                ) &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE event if a property is updated`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val fishNamePayload =
            """
            {
                "value":"Salmon",
                "unitCode":"C1"
            }
            """.trimIndent()

        val jsonLdAttributes = parseAndExpandAttributeFragment(
            "fishName",
            fishNamePayload,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        val updatedDetails = listOf(UpdatedDetails(fishNameAttribute, null, UpdateOperationResult.UPDATED))

        entityEventService.publishPartialUpdateEntityAttributesEvents(
            breedingServiceUri,
            jsonLdAttributes,
            updatedDetails,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> {
                    it as AttributeUpdateEvent
                    it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                        it.entityId == breedingServiceUri &&
                        it.attributeName == "fishName" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(fishNamePayload) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE events if multi instance relationship is updated`() {
        val entityEventService = spyk(EntityEventService(kafkaTemplate, entityService), recordPrivateCalls = true)
        val attributeName = "https://ontology.eglobalmark.com/egm#connectsTo"
        val firstRelationshipPayload =
            """
            {"datasetId":"urn:ngsi-ld:Dataset:connectsTo:1","object":"urn:ngsi-ld:Feeder:018"}
            """.trimIndent()
        val secondRelationshipPayload =
            """
            {"name":{"value":"Salmon"},"object":"urn:ngsi-ld:Feeder:012"}
            """.trimIndent()
        val connectsToPayload =
            """
            [
                $firstRelationshipPayload,
                $secondRelationshipPayload
            ]
            """.trimIndent()
        val jsonLdAttributes = parseAndExpandAttributeFragment(
            "connectsTo",
            connectsToPayload,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        val updatedDetails = listOf(
            UpdatedDetails(attributeName, "urn:ngsi-ld:Dataset:connectsTo:1".toUri(), UpdateOperationResult.UPDATED),
            UpdatedDetails(attributeName, null, UpdateOperationResult.UPDATED)
        )

        entityEventService.publishPartialUpdateEntityAttributesEvents(
            breedingServiceUri,
            jsonLdAttributes,
            updatedDetails,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it as AttributeUpdateEvent
                        it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                            it.entityId == breedingServiceUri &&
                            it.attributeName == "connectsTo" &&
                            (
                                it.datasetId == null || it.datasetId == "urn:ngsi-ld:Dataset:connectsTo:1".toUri()
                                ) &&
                            (
                                it.operationPayload.matchContent(firstRelationshipPayload) ||
                                    it.operationPayload.matchContent(secondRelationshipPayload)
                                ) &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
        confirmVerified()
    }
}
