package com.egm.stellio.search.entity.service

import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.support.EMPTY_PAYLOAD
import com.egm.stellio.shared.model.AttributeCreateEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventsType
import com.egm.stellio.shared.model.ExpandedAttribute
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.UriUtils.toUri
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
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

    private val originalEntity =
        ExpandedEntity(loadSampleData("aquac/breedingService_expanded.jsonld").deserializeAsMap())
    private val updatedEntity =
        ExpandedEntity(loadSampleData("aquac/breedingService_updated_expanded.jsonld").deserializeAsMap())
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
    private val originalFishName1ExpandedAttributeFragment =
        """
        {
          "https://uri.etsi.org/ngsi-ld/datasetId": [
            {
              "@id": "urn:ngsi-ld:Dataset:fishName:1"
            }
          ],
          "@type": [
            "https://uri.etsi.org/ngsi-ld/Property"
          ],
          "https://uri.etsi.org/ngsi-ld/hasValue": [
            {
              "@value": "Trout"
            }
          ]
        }
        """.trimIndent()
    private val originalFishName2ExpandedAttributeFragment =
        """
        {
          "https://uri.etsi.org/ngsi-ld/datasetId": [
            {
              "@id": "urn:ngsi-ld:Dataset:fishName:2"
            }
          ],
          "@type": [
            "https://uri.etsi.org/ngsi-ld/Property"
          ],
          "https://uri.etsi.org/ngsi-ld/hasValue": [
            {
              "@value": "Salmon"
            }
          ]
        }
        """.trimIndent()
    private val fishNameAttributeFragment =
        """
        {
            "type": "Property",
            "datasetId": "$fishName1DatasetUri",
            "value": "Salmon"
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
                EMPTY_PAYLOAD
            )
        )

        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish an ENTITY_CREATE event`() = runTest {
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        entityEventService.publishEntityCreateEvent(
            null,
            originalEntity
        ).join()

        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish an ENTITY_DELETE event`() = runTest {
        val entity = mockk<Entity>(relaxed = true) {
            every { entityId } returns breedingServiceUri
        }
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        entityEventService.publishEntityDeleteEvent(null, entity, ExpandedEntity(emptyMap())).join()

        verify { kafkaTemplate.send("cim.entity._CatchAll", breedingServiceUri.toString(), any()) }
    }

    @Test
    fun `it should publish a single ATTRIBUTE_CREATE event if an attribute was appended`() = runTest {
        val expandedAttribute =
            expandAttribute(fishNumberTerm, fishNumberAttributeFragment, listOf(AQUAC_COMPOUND_CONTEXT))
        entityEventService.publishAttributeChangeEvents(
            "sub",
            ExpandedEntity(emptyMap()),
            updatedEntity,
            listOf(
                SucceededAttributeOperationResult(
                    attributeName = fishNumberProperty,
                    operationStatus = OperationStatus.CREATED,
                    newExpandedValue = expandedAttribute.second[0]
                )
            )
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeCreateEvent> {
                    it.operationType == EventsType.ATTRIBUTE_CREATE &&
                        it.sub == "sub" &&
                        it.entityId == breedingServiceUri &&
                        it.entityTypes == listOf(breedingServiceType) &&
                        it.attributeName == fishNumberProperty &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(serializedAttributePayload(expandedAttribute))
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_CREATE and ATTRIBUTE_UPDATE events if attributes were appended and replaced`() =
        runTest {
            val attributesPayload = """
                {
                    "$fishNameTerm": $fishNameAttributeFragment,
                    "$fishNumberTerm": $fishNumberAttributeFragment
                }
            """.trimIndent()
            val jsonLdAttributes = expandAttributes(attributesPayload, listOf(AQUAC_COMPOUND_CONTEXT))
            val operationResult = listOf(
                SucceededAttributeOperationResult(
                    attributeName = fishNumberProperty,
                    operationStatus = OperationStatus.CREATED,
                    newExpandedValue = jsonLdAttributes[fishNumberProperty]!![0]
                ),
                SucceededAttributeOperationResult(
                    attributeName = fishNameProperty,
                    datasetId = fishName1DatasetUri,
                    operationStatus = OperationStatus.UPDATED,
                    newExpandedValue = jsonLdAttributes[fishNameProperty]!![0]
                )
            )

            entityEventService.publishAttributeChangeEvents(
                null,
                originalEntity,
                updatedEntity,
                operationResult
            ).join()

            verify {
                entityEventService["publishEntityEvent"](
                    match<EntityEvent> { entityEvent ->
                        when (entityEvent) {
                            is AttributeCreateEvent ->
                                listOf(entityEvent).any {
                                    it.entityId == breedingServiceUri &&
                                        it.entityTypes == listOf(breedingServiceType) &&
                                        it.attributeName == fishNumberProperty &&
                                        it.datasetId == null &&
                                        it.operationPayload.matchContent(serializedAttributePayload(jsonLdAttributes))
                                }

                            is AttributeUpdateEvent ->
                                listOf(entityEvent).any {
                                    it.entityId == breedingServiceUri &&
                                        it.entityTypes == listOf(breedingServiceType) &&
                                        it.attributeName == fishNameProperty &&
                                        it.datasetId == fishName1DatasetUri &&
                                        it.previousPayload.matchContent(originalFishName1ExpandedAttributeFragment) &&
                                        it.operationPayload.matchContent(
                                            serializedAttributePayload(jsonLdAttributes, fishNameProperty)
                                        )
                                }

                            else -> false
                        }
                    }
                )
            }
        }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE events if two attributes are replaced`() = runTest {
        val attributesPayload =
            """
            {
                "$fishNameTerm": $fishNameAttributeFragment,
                "$fishNumberTerm": $fishNumberAttributeFragment
            }
            """.trimIndent()
        val jsonLdAttributes = expandAttributes(attributesPayload, listOf(AQUAC_COMPOUND_CONTEXT))
        val operationResult = listOf(
            SucceededAttributeOperationResult(
                attributeName = fishNumberProperty,
                operationStatus = OperationStatus.UPDATED,
                newExpandedValue = jsonLdAttributes[fishNumberProperty]!![0]
            ),
            SucceededAttributeOperationResult(
                attributeName = fishNameProperty,
                datasetId = fishName1DatasetUri,
                operationStatus = OperationStatus.UPDATED,
                newExpandedValue = jsonLdAttributes[fishNameProperty]!![0]
            )
        )

        entityEventService.publishAttributeChangeEvents(
            null,
            originalEntity,
            updatedEntity,
            operationResult
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeUpdateEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                            it.entityId == breedingServiceUri &&
                            it.entityTypes == listOf(breedingServiceType) &&
                            (it.attributeName == fishNameProperty || it.attributeName == fishNumberProperty) &&
                            (it.datasetId == fishName1DatasetUri || it.datasetId == null) &&
                            (
                                it.operationPayload.matchContent(serializedAttributePayload(jsonLdAttributes)) ||
                                    it.operationPayload.matchContent(
                                        serializedAttributePayload(jsonLdAttributes, fishNameProperty)
                                    )
                                )
                    }
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE events if a multi-attribute is replaced`() = runTest {
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
        val operationResult = listOf(
            SucceededAttributeOperationResult(
                attributeName = fishNameProperty,
                datasetId = fishName1DatasetUri,
                operationStatus = OperationStatus.UPDATED,
                newExpandedValue = jsonLdAttributes[fishNameProperty]!![0]
            ),
            SucceededAttributeOperationResult(
                attributeName = fishNameProperty,
                datasetId = fishName2DatasetUri,
                operationStatus = OperationStatus.UPDATED,
                newExpandedValue = jsonLdAttributes[fishNameProperty]!![1]
            )
        )

        entityEventService.publishAttributeChangeEvents(
            null,
            originalEntity,
            updatedEntity,
            operationResult
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeUpdateEvent> { entityEvent ->
                    listOf(entityEvent).all {
                        it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                            it.entityId == breedingServiceUri &&
                            it.entityTypes == listOf(breedingServiceType) &&
                            it.attributeName == fishNameProperty &&
                            (it.datasetId == fishName1DatasetUri || it.datasetId == fishName2DatasetUri) &&
                            (
                                it.previousPayload.matchContent(originalFishName1ExpandedAttributeFragment) ||
                                    it.previousPayload.matchContent(originalFishName2ExpandedAttributeFragment)
                                ) &&
                            (
                                it.operationPayload.matchContent(
                                    serializedAttributePayload(jsonLdAttributes, fishNameProperty)
                                ) ||
                                    it.operationPayload.matchContent(
                                        serializedAttributePayload(jsonLdAttributes, fishNameProperty, 1)
                                    )
                                )
                    }
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_UPDATE event if an attribute is updated`() = runTest {
        val expandedAttribute = expandAttribute(
            fishNameTerm,
            fishNameAttributeFragment,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        val operationResult = listOf(
            SucceededAttributeOperationResult(
                attributeName = fishNameProperty,
                datasetId = fishName1DatasetUri,
                operationStatus = OperationStatus.UPDATED,
                newExpandedValue = expandedAttribute.second[0]
            )
        )

        entityEventService.publishAttributeChangeEvents(
            null,
            originalEntity,
            updatedEntity,
            operationResult
        ).join()

        verify {
            entityEventService["publishEntityEvent"](
                match<AttributeUpdateEvent> {
                    it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                        it.entityId == breedingServiceUri &&
                        it.entityTypes == listOf(breedingServiceType) &&
                        it.attributeName == fishNameProperty &&
                        it.datasetId == fishName1DatasetUri &&
                        it.operationPayload.matchContent(serializedAttributePayload(expandedAttribute))
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_DELETE event if an attribute has been deleted as part of an update `() = runTest {
        entityEventService.publishAttributeChangeEvents(
            null,
            originalEntity,
            updatedEntity,
            listOf(
                SucceededAttributeOperationResult(
                    fishNameProperty,
                    fishName1DatasetUri,
                    OperationStatus.DELETED,
                    emptyMap()
                )
            )
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
                            it.previousPayload.matchContent(originalFishName1ExpandedAttributeFragment)
                    }
                }
            )
        }
    }

    @Test
    fun `it should publish ATTRIBUTE_DELETE event if an instance of an attribute is deleted`() = runTest {
        entityEventService.publishAttributeDeleteEvent(
            null,
            originalEntity,
            updatedEntity,
            SucceededAttributeOperationResult(
                fishNameProperty,
                fishName1DatasetUri,
                OperationStatus.DELETED,
                emptyMap()
            )
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
                            it.previousPayload.matchContent(originalFishName1ExpandedAttributeFragment)
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
