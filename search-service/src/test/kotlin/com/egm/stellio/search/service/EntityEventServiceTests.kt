package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.UpdateOperationResult
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.model.UpdatedDetails
import com.egm.stellio.search.support.EMPTY_PAYLOAD
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.web.DEFAULT_TENANT_URI
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import java.util.concurrent.CompletableFuture

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityEventService::class])
@ActiveProfiles("test")
class EntityEventServiceTests {

    @SpykBean
    private lateinit var entityEventService: EntityEventService

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean(relaxed = true)
    private lateinit var entityPayloadService: EntityPayloadService

    private val breedingServiceUri = "urn:ngsi-ld:BreedingService:0214".toUri()
    private val breedingServiceType = "https://ontology.eglobalmark.com/aquac#BreedingService"
    private val fishNameTerm = "fishName"
    private val fishNameProperty = "https://ontology.eglobalmark.com/aquac#$fishNameTerm"
    private val fishNumberTerm = "fishNumber"
    private val fishNumberProperty = "https://ontology.eglobalmark.com/aquac#$fishNumberTerm"
    private val fishName1DatasetUri = "urn:ngsi-ld:Dataset:fishName:1".toUri()
    private val fishName2DatasetUri = "urn:ngsi-ld:Dataset:fishName:2".toUri()
    private val fishNumberAttributeFragment =
        """
        {
            "type": "Property",
            "value": 120
        }            
        """.trimIndent()
    private val fishNameAttributeFragment =
        """
        {
            "type": "Property",
            "datasetId": "$fishName1DatasetUri",
            "value": 50
        }
        """.trimIndent()

    @Test
    fun `it should publish all events on the catch-all topic`() {
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        entityEventService.publishEntityEvent(
            EntityCreateEvent(
                null,
                DEFAULT_TENANT_URI,
                breedingServiceUri,
                listOf(breedingServiceType),
                EMPTY_PAYLOAD,
                listOf(AQUAC_COMPOUND_CONTEXT)
            )
        )

        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish an ENTITY_CREATE event`() = runTest {
        coEvery {
            entityEventService.getSerializedEntity(any())
        } returns Pair(listOf(breedingServiceType), EMPTY_PAYLOAD).right()
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        entityEventService.publishEntityCreateEvent(
            null,
            breedingServiceUri,
            listOf(breedingServiceType),
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        coVerify { entityEventService.getSerializedEntity(eq(breedingServiceUri)) }
        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish an ENTITY_REPLACE event`() = runTest {
        coEvery {
            entityEventService.getSerializedEntity(any())
        } returns Pair(listOf(breedingServiceType), EMPTY_PAYLOAD).right()
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        entityEventService.publishEntityReplaceEvent(
            null,
            breedingServiceUri,
            listOf(breedingServiceType),
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        coVerify { entityEventService.getSerializedEntity(eq(breedingServiceUri)) }
        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish an ENTITY_DELETE event`() = runTest {
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        entityEventService.publishEntityDeleteEvent(
            null,
            breedingServiceUri,
            listOf(breedingServiceType),
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish a single ATTRIBUTE_APPEND event if an attribute was appended`() = runTest {
        val entityPayload = mockk<EntityPayload>(relaxed = true)
        coEvery { entityPayloadService.retrieve(breedingServiceUri) } returns entityPayload.right()
        every { entityPayload.types } returns listOf(breedingServiceType)

        val expandedAttribute =
            expandAttribute(fishNumberTerm, fishNumberAttributeFragment, listOf(AQUAC_COMPOUND_CONTEXT))
        entityEventService.publishAttributeChangeEvents(
            "sub",
            breedingServiceUri,
            expandedAttribute.toExpandedAttributes(),
            UpdateResult(
                listOf(UpdatedDetails(fishNumberProperty, null, UpdateOperationResult.APPENDED)),
                emptyList()
            ),
            true,
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeAppendEvent> {
                    it.operationType == EventsType.ATTRIBUTE_APPEND &&
                        it.sub == "sub" &&
                        it.entityId == breedingServiceUri &&
                        it.entityTypes == listOf(breedingServiceType) &&
                        it.attributeName == fishNumberProperty &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(serializedAttributePayload(expandedAttribute)) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }
    }

    @Test
    fun `it should publish a single ATTRIBUTE_REPLACE event if an attribute was replaced`() = runTest {
        val entityPayload = mockk<EntityPayload>(relaxed = true)
        coEvery { entityPayloadService.retrieve(breedingServiceUri) } returns entityPayload.right()
        every { entityPayload.types } returns listOf(breedingServiceType)

        val expandedAttribute =
            expandAttribute(fishNumberTerm, fishNumberAttributeFragment, listOf(AQUAC_COMPOUND_CONTEXT))
        entityEventService.publishAttributeChangeEvents(
            null,
            breedingServiceUri,
            expandedAttribute.toExpandedAttributes(),
            UpdateResult(
                listOf(UpdatedDetails(fishNumberProperty, null, UpdateOperationResult.REPLACED)),
                emptyList()
            ),
            true,
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeReplaceEvent> {
                    it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                        it.sub == null &&
                        it.entityId == breedingServiceUri &&
                        it.entityTypes == listOf(breedingServiceType) &&
                        it.attributeName == fishNumberProperty &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(serializedAttributePayload(expandedAttribute)) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_APPEND and ATTRIBUTE_REPLACE events if attributes were appended and replaced`() =
        runTest {
            val entityPayload = mockk<EntityPayload>(relaxed = true)
            val attributesPayload =
                """
                {
                    "$fishNameTerm": $fishNameAttributeFragment,
                    "$fishNumberTerm": $fishNumberAttributeFragment
                }
                """.trimIndent()
            val jsonLdAttributes = expandJsonLdFragment(attributesPayload, listOf(AQUAC_COMPOUND_CONTEXT))
            val appendResult = UpdateResult(
                listOf(
                    UpdatedDetails(fishNumberProperty, null, UpdateOperationResult.APPENDED),
                    UpdatedDetails(fishNameProperty, fishName1DatasetUri, UpdateOperationResult.REPLACED)
                ),
                emptyList()
            )

            coEvery { entityPayloadService.retrieve(breedingServiceUri) } returns entityPayload.right()
            every { entityPayload.types } returns listOf(breedingServiceType)

            entityEventService.publishAttributeChangeEvents(
                null,
                breedingServiceUri,
                jsonLdAttributes,
                appendResult,
                true,
                listOf(AQUAC_COMPOUND_CONTEXT)
            ).join()

            verify {
                entityEventService["publishEntityEvent"](
                    match<EntityEvent> { entityEvent ->
                        when (entityEvent) {
                            is AttributeAppendEvent ->
                                listOf(entityEvent).any {
                                    it.entityId == breedingServiceUri &&
                                        it.entityTypes == listOf(breedingServiceType) &&
                                        it.attributeName == fishNumberProperty &&
                                        it.datasetId == null &&
                                        it.operationPayload.matchContent(
                                            serializedAttributePayload(jsonLdAttributes)
                                        ) &&
                                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                                }

                            is AttributeReplaceEvent ->
                                listOf(entityEvent).any {
                                    it.entityId == breedingServiceUri &&
                                        it.entityTypes == listOf(breedingServiceType) &&
                                        it.attributeName == fishNameProperty &&
                                        it.datasetId == fishName1DatasetUri &&
                                        it.operationPayload.matchContent(
                                            serializedAttributePayload(jsonLdAttributes, fishNameProperty)
                                        ) &&
                                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                                }

                            else -> false
                        }
                    }
                )
            }
        }

    @Test
    fun `it should publish ATTRIBUTE_REPLACE events if two attributes are replaced`() = runTest {
        val entityPayload = mockk<EntityPayload>(relaxed = true)
        val attributesPayload =
            """
            {
                "$fishNameTerm": $fishNameAttributeFragment,
                "$fishNumberTerm": $fishNumberAttributeFragment
            }
            """.trimIndent()
        val jsonLdAttributes = expandJsonLdFragment(attributesPayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNameProperty, fishName1DatasetUri, UpdateOperationResult.REPLACED),
                UpdatedDetails(fishNumberProperty, null, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf()
        )

        coEvery { entityPayloadService.retrieve(breedingServiceUri) } returns entityPayload.right()
        every { entityPayload.types } returns listOf(breedingServiceType)

        entityEventService.publishAttributeChangeEvents(
            null,
            breedingServiceUri,
            jsonLdAttributes,
            updateResult,
            true,
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeReplaceEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                            it.entityId == breedingServiceUri &&
                            it.entityTypes == listOf(breedingServiceType) &&
                            (it.attributeName == fishNameProperty || it.attributeName == fishNumberProperty) &&
                            (it.datasetId == fishName1DatasetUri || it.datasetId == null) &&
                            (
                                it.operationPayload.matchContent(serializedAttributePayload(jsonLdAttributes)) ||
                                    it.operationPayload.matchContent(
                                        serializedAttributePayload(jsonLdAttributes, fishNameProperty)
                                    )
                                ) &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_REPLACE events if a multi-attribute is replaced`() = runTest {
        val entityPayload = mockk<EntityPayload>(relaxed = true)
        val fishNameAttributeFragment2 =
            """
            {
                "type":"Property",
                "datasetId":"$fishName2DatasetUri",
                "value":"Salmon2"
            }            
            """.trimIndent()
        val attributePayload =
            """
            {
                "$fishNameTerm": [$fishNameAttributeFragment, $fishNameAttributeFragment2]
            }
            """.trimIndent()
        val jsonLdAttributes = expandJsonLdFragment(attributePayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNameProperty, fishName1DatasetUri, UpdateOperationResult.REPLACED),
                UpdatedDetails(fishNameProperty, fishName2DatasetUri, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf()
        )

        coEvery { entityPayloadService.retrieve(breedingServiceUri) } returns entityPayload.right()
        every { entityPayload.types } returns listOf(breedingServiceType)

        entityEventService.publishAttributeChangeEvents(
            null,
            breedingServiceUri,
            jsonLdAttributes,
            updateResult,
            true,
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeReplaceEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                            it.entityId == breedingServiceUri &&
                            it.entityTypes == listOf(breedingServiceType) &&
                            it.attributeName == fishNameProperty &&
                            (it.datasetId == fishName1DatasetUri || it.datasetId == fishName2DatasetUri) &&
                            (
                                it.operationPayload.matchContent(
                                    serializedAttributePayload(jsonLdAttributes, fishNameProperty)
                                ) ||
                                    it.operationPayload.matchContent(
                                        serializedAttributePayload(jsonLdAttributes, fishNameProperty, 1)
                                    )
                                ) &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE event if an attribute is updated`() = runTest {
        val entityPayload = mockk<EntityPayload>(relaxed = true)

        val expandedAttribute = expandAttribute(
            fishNameTerm,
            fishNameAttributeFragment,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        val updatedDetails = listOf(
            UpdatedDetails(fishNameProperty, fishName1DatasetUri, UpdateOperationResult.UPDATED)
        )

        coEvery { entityPayloadService.retrieve(breedingServiceUri) } returns entityPayload.right()
        every { entityPayload.types } returns listOf(breedingServiceType)

        entityEventService.publishAttributeChangeEvents(
            null,
            breedingServiceUri,
            expandedAttribute.toExpandedAttributes(),
            UpdateResult(updatedDetails, emptyList()),
            false,
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeUpdateEvent> {
                    it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                        it.entityId == breedingServiceUri &&
                        it.entityTypes == listOf(breedingServiceType) &&
                        it.attributeName == fishNameProperty &&
                        it.datasetId == fishName1DatasetUri &&
                        it.operationPayload.matchContent(serializedAttributePayload(expandedAttribute)) &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_DELETE_ALL_INSTANCE event if all instances of an attribute are deleted`() =
        runTest {
            val entityPayload = mockk<EntityPayload>(relaxed = true)

            coEvery { entityPayloadService.retrieve(breedingServiceUri) } returns entityPayload.right()
            every { entityPayload.types } returns listOf(breedingServiceType)

            entityEventService.publishAttributeDeleteEvent(
                null,
                breedingServiceUri,
                fishNameProperty,
                null,
                true,
                listOf(AQUAC_COMPOUND_CONTEXT)
            ).join()

            verify {
                entityEventService["publishEntityEvent"](
                    match<AttributeDeleteAllInstancesEvent> { entityEvent ->
                        listOf(entityEvent).all {
                            it.operationType == EventsType.ATTRIBUTE_DELETE_ALL_INSTANCES &&
                                it.entityId == breedingServiceUri &&
                                it.entityTypes == listOf(breedingServiceType) &&
                                it.attributeName == fishNameProperty &&
                                it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                        }
                    }
                )
            }
        }

    @Test
    fun `it should publish ATTRIBUTE_DELETE event if an instance of an attribute is deleted`() = runTest {
        val entityPayload = mockk<EntityPayload>(relaxed = true)

        coEvery { entityPayloadService.retrieve(breedingServiceUri) } returns entityPayload.right()
        every { entityPayload.types } returns listOf(breedingServiceType)

        entityEventService.publishAttributeDeleteEvent(
            null,
            breedingServiceUri,
            fishNameProperty,
            fishName1DatasetUri,
            false,
            listOf(AQUAC_COMPOUND_CONTEXT)
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeDeleteEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_DELETE &&
                            it.entityId == breedingServiceUri &&
                            it.entityTypes == listOf(breedingServiceType) &&
                            it.attributeName == fishNameProperty &&
                            it.datasetId == fishName1DatasetUri &&
                            it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                    }
                }
            )
        }
    }

    private fun serializedAttributePayload(
        jsonLdAttributes: ExpandedAttributeInstance,
        attributeName: ExpandedTerm = fishNumberProperty,
        index: Int = 0
    ): String =
        serializeObject(jsonLdAttributes[attributeName]!![index])

    private fun serializedAttributePayload(
        expandedAttribute: ExpandedAttribute,
        index: Int = 0
    ): String =
        serializeObject(expandedAttribute.second[index])
}
