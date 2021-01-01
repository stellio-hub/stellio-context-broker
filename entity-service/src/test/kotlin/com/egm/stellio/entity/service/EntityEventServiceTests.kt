package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(classes = [EntityEventService::class])
@ActiveProfiles("test")
class EntityEventServiceTests {

    @Autowired
    private lateinit var entityEventService: EntityEventService

    @MockkBean(relaxed = true)
    private lateinit var resolver: BinderAwareChannelResolver

    private val entityUri = "urn:ngsi-ld:BreedingService:0214".toUri()
    private val fishNumberAttribute = "https://ontology.eglobalmark.com/aquac#fishNumber"
    private val fishSizeAttribute = "https://ontology.eglobalmark.com/aquac#fishSize"

    @Test
    fun `it should publish an ENTITY_CREATE event`() {
        val event = EntityCreateEvent(
            entityUri,
            "operationPayload",
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        every { resolver.resolveDestination(any()).send(any()) } returns true

        entityEventService.publishEntityEvent(event, "Vehicle")

        verify { resolver.resolveDestination("cim.entity.Vehicle") }
    }

    @Test
    fun `it should publish an ATTRIBUTE_APPEND event if an attribute were appended`() {
        val entityEventService = spyk(EntityEventService(resolver), recordPrivateCalls = true)
        val jsonLdAttributes = mapOf(fishNumberAttribute to 120)
        val appendResult = UpdateResult(
            listOf(UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.APPENDED)),
            emptyList()
        )

        entityEventService.publishAppendEntityAttributesEvents(
            entityUri,
            jsonLdAttributes,
            appendResult,
            mockkClass(JsonLdEntity::class, relaxed = true),
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> {
                    it as AttributeAppendEvent
                    it.operationType == EventsType.ATTRIBUTE_APPEND &&
                        it.entityId == entityUri &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent("{\"fishNumber\":120}") &&
                        it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                },
                any<String>()
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should publish ATTRIBUTE_APPEND and ATTRIBUTE_REPLACE events if attributes were appended and replaced`() {
        val entityEventService = spyk(EntityEventService(resolver), recordPrivateCalls = true)
        val jsonLdAttributes = mapOf(fishNumberAttribute to 120, fishSizeAttribute to 50)
        val appendResult = UpdateResult(
            listOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.APPENDED
                ),
                UpdatedDetails(
                    fishSizeAttribute,
                    "urn:ngsi-ld:Dataset:fishSize:1".toUri(),
                    UpdateOperationResult.REPLACED
                )
            ),
            emptyList()
        )

        entityEventService.publishAppendEntityAttributesEvents(
            entityUri,
            jsonLdAttributes,
            appendResult,
            mockkClass(JsonLdEntity::class, relaxed = true),
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        verify {
            entityEventService["publishEntityEvent"](
                match<EntityEvent> {
                    if (it is AttributeAppendEvent) {
                        listOf(it).any {
                            it.operationType == EventsType.ATTRIBUTE_APPEND &&
                                it.entityId == entityUri &&
                                it.attributeName == "fishNumber" &&
                                it.datasetId == null &&
                                it.operationPayload.matchContent("{\"fishNumber\":120}") &&
                                it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                        }
                    } else {
                        listOf(it).any {
                            it as AttributeReplaceEvent
                            it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                                it.entityId == entityUri &&
                                it.attributeName == "fishSize" &&
                                it.datasetId == "urn:ngsi-ld:Dataset:fishSize:1".toUri() &&
                                it.operationPayload.matchContent("{\"fishSize\":50}") &&
                                it.contexts == listOf(AQUAC_COMPOUND_CONTEXT)
                        }
                    }
                },
                any<String>()
            )
        }

        confirmVerified()
    }
}
