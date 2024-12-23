package com.egm.stellio.search.entity.service

import arrow.core.right
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.UpdateOperationResult
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.model.UpdatedDetails
import com.egm.stellio.search.support.EMPTY_PAYLOAD
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteAllInstancesEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventsType
import com.egm.stellio.shared.model.ExpandedAttribute
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.toExpandedAttributes
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import java.util.concurrent.CompletableFuture

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityEventService::class])
@ActiveProfiles("test")
class EntityEventServiceTests {

    @SpykBean
    private lateinit var entityEventService: EntityEventService

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean(relaxed = true)
    private lateinit var entityQueryService: EntityQueryService

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
                DEFAULT_TENANT_NAME,
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
            listOf(breedingServiceType)
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
            listOf(breedingServiceType)
        ).join()

        coVerify { entityEventService.getSerializedEntity(eq(breedingServiceUri)) }
        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish an ENTITY_DELETE event`() = runTest {
        val entity = mockk<Entity>(relaxed = true) {
            every { entityId } returns breedingServiceUri
        }
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        entityEventService.publishEntityDeleteEvent(null, entity, deletedEntityPayload).join()

        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish a single ATTRIBUTE_APPEND event if an attribute was appended`() = runTest {
        val entity = mockk<Entity>(relaxed = true)
        coEvery { entityQueryService.retrieve(breedingServiceUri) } returns entity.right()
        every { entity.types } returns listOf(breedingServiceType)

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
            true
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
                        it.contexts.isEmpty()
                }
            )
        }
    }

    @Test
    fun `it should publish a single ATTRIBUTE_REPLACE event if an attribute was replaced`() = runTest {
        val entity = mockk<Entity>(relaxed = true)
        coEvery { entityQueryService.retrieve(breedingServiceUri) } returns entity.right()
        every { entity.types } returns listOf(breedingServiceType)

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
            true
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
                        it.contexts.isEmpty()
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_APPEND and ATTRIBUTE_REPLACE events if attributes were appended and replaced`() =
        runTest {
            val entity = mockk<Entity>(relaxed = true)
            val attributesPayload =
                """
                {
                    "$fishNameTerm": $fishNameAttributeFragment,
                    "$fishNumberTerm": $fishNumberAttributeFragment
                }
                """.trimIndent()
            val jsonLdAttributes = expandAttributes(attributesPayload, listOf(AQUAC_COMPOUND_CONTEXT))
            val appendResult = UpdateResult(
                listOf(
                    UpdatedDetails(fishNumberProperty, null, UpdateOperationResult.APPENDED),
                    UpdatedDetails(fishNameProperty, fishName1DatasetUri, UpdateOperationResult.REPLACED)
                ),
                emptyList()
            )

            coEvery { entityQueryService.retrieve(breedingServiceUri) } returns entity.right()
            every { entity.types } returns listOf(breedingServiceType)

            entityEventService.publishAttributeChangeEvents(
                null,
                breedingServiceUri,
                jsonLdAttributes,
                appendResult,
                true
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
                                        it.contexts.isEmpty()
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
                                        it.contexts.isEmpty()
                                }

                            else -> false
                        }
                    }
                )
            }
        }

    @Test
    fun `it should publish ATTRIBUTE_REPLACE events if two attributes are replaced`() = runTest {
        val entity = mockk<Entity>(relaxed = true)
        val attributesPayload =
            """
            {
                "$fishNameTerm": $fishNameAttributeFragment,
                "$fishNumberTerm": $fishNumberAttributeFragment
            }
            """.trimIndent()
        val jsonLdAttributes = expandAttributes(attributesPayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNameProperty, fishName1DatasetUri, UpdateOperationResult.REPLACED),
                UpdatedDetails(fishNumberProperty, null, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf()
        )

        coEvery { entityQueryService.retrieve(breedingServiceUri) } returns entity.right()
        every { entity.types } returns listOf(breedingServiceType)

        entityEventService.publishAttributeChangeEvents(
            null,
            breedingServiceUri,
            jsonLdAttributes,
            updateResult,
            true
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
                            it.contexts.isEmpty()
                    }
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_REPLACE events if a multi-attribute is replaced`() = runTest {
        val entity = mockk<Entity>(relaxed = true)
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
        val jsonLdAttributes = expandAttributes(attributePayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNameProperty, fishName1DatasetUri, UpdateOperationResult.REPLACED),
                UpdatedDetails(fishNameProperty, fishName2DatasetUri, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf()
        )

        coEvery { entityQueryService.retrieve(breedingServiceUri) } returns entity.right()
        every { entity.types } returns listOf(breedingServiceType)

        entityEventService.publishAttributeChangeEvents(
            null,
            breedingServiceUri,
            jsonLdAttributes,
            updateResult,
            true
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
                            it.contexts.isEmpty()
                    }
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE event if an attribute is updated`() = runTest {
        val entity = mockk<Entity>(relaxed = true)

        val expandedAttribute = expandAttribute(
            fishNameTerm,
            fishNameAttributeFragment,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        val updatedDetails = listOf(
            UpdatedDetails(fishNameProperty, fishName1DatasetUri, UpdateOperationResult.UPDATED)
        )

        coEvery { entityQueryService.retrieve(breedingServiceUri) } returns entity.right()
        every { entity.types } returns listOf(breedingServiceType)

        entityEventService.publishAttributeChangeEvents(
            null,
            breedingServiceUri,
            expandedAttribute.toExpandedAttributes(),
            UpdateResult(updatedDetails, emptyList()),
            false
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
                        it.contexts.isEmpty()
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_DELETE_ALL_INSTANCE event if all instances of an attribute are deleted`() =
        runTest {
            val entity = mockk<Entity>(relaxed = true)

            coEvery { entityQueryService.retrieve(breedingServiceUri) } returns entity.right()
            every { entity.types } returns listOf(breedingServiceType)

            entityEventService.publishAttributeDeleteEvent(
                null,
                breedingServiceUri,
                fishNameProperty,
                null,
                true
            ).join()

            verify {
                entityEventService["publishEntityEvent"](
                    match<AttributeDeleteAllInstancesEvent> { entityEvent ->
                        listOf(entityEvent).all {
                            it.operationType == EventsType.ATTRIBUTE_DELETE_ALL_INSTANCES &&
                                it.entityId == breedingServiceUri &&
                                it.entityTypes == listOf(breedingServiceType) &&
                                it.attributeName == fishNameProperty &&
                                it.contexts.isEmpty()
                        }
                    }
                )
            }
        }

    @Test
    fun `it should publish ATTRIBUTE_DELETE event if an instance of an attribute is deleted`() = runTest {
        val entity = mockk<Entity>(relaxed = true)

        coEvery { entityQueryService.retrieve(breedingServiceUri) } returns entity.right()
        every { entity.types } returns listOf(breedingServiceType)

        entityEventService.publishAttributeDeleteEvent(
            null,
            breedingServiceUri,
            fishNameProperty,
            fishName1DatasetUri,
            false
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
                            it.contexts.isEmpty()
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
