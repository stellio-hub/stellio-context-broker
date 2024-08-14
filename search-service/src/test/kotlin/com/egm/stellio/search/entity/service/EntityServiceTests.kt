package com.egm.stellio.search.entity.service

import arrow.core.right
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.entity.model.*
import com.egm.stellio.search.support.EMPTY_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class EntityServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityService: EntityService

    @Autowired
    private lateinit var entityQueryService: EntityQueryService

    @MockkBean
    private lateinit var entityAttributeService: EntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val entity01Uri = "urn:ngsi-ld:Entity:01".toUri()
    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val now = ngsiLdDateTime()

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
    }

    @Test
    fun `it should create an entity payload from string if none existed yet`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                ).shouldSucceed()
            }
    }

    @Test
    fun `it should create an entity payload from an NGSI-LD Entity if none existed yet`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityService.createEntityPayload(ngsiLdEntity, jsonLdEntity, now)
            .shouldSucceed()
    }

    @Test
    fun `it should only create an entity payload for a minimal entity`() = runTest {
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any()) } returns Job()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals("urn:ngsi-ld:BeeHive:TESTC".toUri(), it.entityId)
                assertEquals(listOf(BEEHIVE_TYPE), it.types)
            }

        coVerify {
            entityAttributeService.createAttributes(
                any(),
                any(),
                emptyList(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
            entityEventService.publishEntityCreateEvent(
                eq("0123456789-1234-5678-987654321"),
                eq(beehiveTestCId),
                any()
            )
        }
    }

    @Test
    fun `it should not create an entity payload if one already existed`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE)).sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityService.createEntityPayload(
            ngsiLdEntity,
            jsonLdEntity,
            now,
        )

        assertThrows<DataIntegrityViolationException> {
            entityService.createEntityPayload(
                ngsiLdEntity,
                jsonLdEntity,
                now,
            )
        }
    }

    @Test
    fun `it should merge an entity`() = runTest {
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            entityAttributeService.mergeAttributes(any(), any(), any(), any(), any(), any())
        } returns UpdateResult(
            listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.APPENDED)),
            emptyList()
        ).right()
        coEvery {
            entityAttributeService.getForEntity(any(), any(), any())
        } returns emptyList()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val (jsonLdEntity, _) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()

        entityService.mergeEntity(
            beehiveTestCId,
            jsonLdEntity.getModifiableMembers(),
            now,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt != null)
            }

        coVerify {
            entityAttributeService.createAttributes(
                any(),
                any(),
                emptyList(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
            entityAttributeService.mergeAttributes(
                eq(beehiveTestCId),
                any(),
                any(),
                any(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
            entityAttributeService.getForEntity(
                eq(beehiveTestCId),
                emptySet(),
                emptySet()
            )
        }
    }

    @Test
    fun `it should merge an entity with new types`() = runTest {
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            entityAttributeService.mergeAttributes(any(), any(), any(), any(), any(), any())
        } returns UpdateResult(
            listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.APPENDED)),
            emptyList()
        ).right()
        coEvery {
            entityAttributeService.getForEntity(any(), any(), any())
        } returns emptyList()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val expandedAttributes = expandAttributes(
            loadSampleData("fragments/beehive_merge_entity_multiple_types.jsonld"),
            APIC_COMPOUND_CONTEXTS
        )

        entityService.mergeEntity(
            beehiveTestCId,
            expandedAttributes,
            now,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.types.containsAll(setOf(BEEHIVE_TYPE, NGSILD_DEFAULT_VOCAB + "Distribution")))
            }
    }

    @Test
    fun `it should merge an entity with new types and scopes`() = runTest {
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            entityAttributeService.mergeAttributes(any(), any(), any(), any(), any(), any())
        } returns UpdateResult(
            listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.APPENDED)),
            emptyList()
        ).right()
        coEvery {
            entityAttributeService.partialUpdateAttribute(any(), any(), any(), any())
        } returns EMPTY_UPDATE_RESULT.right()
        coEvery {
            entityAttributeService.getForEntity(any(), any(), any())
        } returns emptyList()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val expandedAttributes = expandAttributes(
            loadSampleData("fragments/beehive_merge_entity_multiple_types_and_scopes.jsonld"),
            APIC_COMPOUND_CONTEXTS
        )

        entityService.mergeEntity(
            beehiveTestCId,
            expandedAttributes,
            now,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.types.containsAll(setOf(BEEHIVE_TYPE, NGSILD_DEFAULT_VOCAB + "Distribution")))
                assertTrue(it.scopes?.containsAll(setOf("/Nantes/BottiereChenaie", "/Agri/Beekeeping")) ?: false)
            }
    }

    @Test
    fun `it should replace an entity payload if entity previously existed`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityService.createEntityPayload(ngsiLdEntity, jsonLdEntity, now).shouldSucceed()

        entityService.replaceEntityPayload(ngsiLdEntity, jsonLdEntity, now).shouldSucceed()
    }

    @Test
    fun `it should replace an entity`() = runTest {
        val beehiveURI = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery { entityAttributeService.deleteAttributes(any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val (newExpandedEntity, newNgsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()

        entityService.replaceEntity(
            beehiveURI,
            newNgsiLdEntity,
            newExpandedEntity,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt != null)
                assertEquals(8, it.payload.deserializeAsMap().size)
            }

        coVerify {
            entityAttributeService.deleteAttributes(beehiveURI)
            entityAttributeService.createAttributes(
                any(),
                any(),
                emptyList(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
        }
    }

    @Test
    fun `it should replace an attribute`() = runTest {
        coEvery { entityAttributeService.getForEntity(any(), any(), any()) } returns emptyList()
        coEvery {
            entityAttributeService.replaceAttribute(any(), any(), any(), any(), any())
        } returns UpdateResult(
            updated = listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.REPLACED)),
            notUpdated = emptyList()
        ).right()

        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityService.createEntityPayload(ngsiLdEntity, jsonLdEntity, now).shouldSucceed()

        val expandedAttribute = expandAttribute(
            loadSampleData("fragments/beehive_new_incoming_property.json"),
            APIC_COMPOUND_CONTEXTS
        )

        entityService.replaceAttribute(beehiveTestCId, expandedAttribute, "0123456789-1234-5678-987654321")
            .shouldSucceedWith {
                it.updated.size == 1 &&
                    it.notUpdated.isEmpty() &&
                    it.updated[0].attributeName == INCOMING_PROPERTY &&
                    it.updated[0].updateOperationResult == UpdateOperationResult.REPLACED
            }
    }

    @Test
    fun `it should add a type to an entity`() = runTest {
        val expandedPayload = loadSampleData("beehive_expanded.jsonld")
        expandedPayload.sampleDataToNgsiLdEntity().map {
            entityService.createEntityPayload(
                it.second,
                it.first,
                now
            )
        }

        entityService.updateTypes(beehiveTestCId, listOf(BEEHIVE_TYPE, APIARY_TYPE), ngsiLdDateTime(), false)
            .shouldSucceedWith {
                assertTrue(it.isSuccessful())
                assertEquals(1, it.updated.size)
                val updatedDetails = it.updated[0]
                assertEquals(JSONLD_TYPE, updatedDetails.attributeName)
                assertEquals(UpdateOperationResult.APPENDED, updatedDetails.updateOperationResult)
            }

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(listOf(BEEHIVE_TYPE, APIARY_TYPE), it.types)
                assertEquals(
                    listOf(BEEHIVE_TYPE, APIARY_TYPE),
                    it.payload.asString().deserializeExpandedPayload()[JSONLD_TYPE]
                )
            }
    }

    @Test
    fun `it should add a type to an entity even if existing types are not in the list of types to add`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }
        entityService.updateTypes(entity01Uri, listOf(APIARY_TYPE), ngsiLdDateTime(), false)
            .shouldSucceed()

        entityQueryService.retrieve(entity01Uri)
            .shouldSucceedWith {
                assertEquals(listOf(BEEHIVE_TYPE, APIARY_TYPE), it.types)
                assertEquals(
                    listOf(BEEHIVE_TYPE, APIARY_TYPE),
                    it.payload.asString().deserializeExpandedPayload()[JSONLD_TYPE]
                )
            }
    }

    @Test
    fun `it should upsert an entity payload if one already existed`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityService.upsertEntityPayload(entity01Uri, EMPTY_PAYLOAD)
            .shouldSucceed()
    }

    @Test
    fun `it should delete an entity payload`() = runTest {
        coEvery { entityAttributeService.deleteAttributes(any()) } returns Unit.right()

        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityService.deleteEntityPayload(entity01Uri)
            .shouldSucceedWith {
                assertEquals(entity01Uri, it.entityId)
                assertNotNull(it.payload)
            }

        // if correctly deleted, we should be able to create a new one
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                ).shouldSucceed()
            }
    }

    @Test
    fun `it should remove the scopes from an entity`() = runTest {
        coEvery {
            entityAttributeService.addAttribute(any(), any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            entityAttributeService.getForEntity(any(), any(), any())
        } returns emptyList()

        loadSampleData("beehive_with_scope.jsonld")
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityService.deleteAttribute(beehiveTestCId, NGSILD_SCOPE_PROPERTY, null)
            .shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertNull(it.scopes)
            }
    }
}
