package com.egm.stellio.search.entity.service

import arrow.core.left
import arrow.core.right
import arrow.core.toOption
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.entity.model.EMPTY_UPDATE_RESULT
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.UpdateOperationResult
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.model.UpdatedDetails
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIARY_TYPE
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DELETED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.loadAndPrepareSampleData
import com.egm.stellio.shared.util.loadMinimalEntity
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.sampleDataToNgsiLdEntity
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
class EntityServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityService: EntityService

    @Autowired
    private lateinit var entityQueryService: EntityQueryService

    @MockkBean
    private lateinit var entityAttributeService: EntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val entity01Uri = "urn:ngsi-ld:Entity:01".toUri()
    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val sub = "0123456789-1234-5678-987654321"
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
        coEvery { authorizationService.userCanCreateEntities(any()) } returns Unit.right()
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any()) } returns Job()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery { authorizationService.createOwnerRight(any(), any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            sub
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(beehiveTestCId, it.entityId)
                assertEquals(listOf(BEEHIVE_TYPE), it.types)
            }

        coVerify {
            authorizationService.userCanCreateEntities(eq(sub.toOption()))
            entityAttributeService.createAttributes(
                any(),
                any(),
                emptyList(),
                any(),
                eq(sub)
            )
            entityEventService.publishEntityCreateEvent(
                eq(sub),
                eq(beehiveTestCId),
                any()
            )
            authorizationService.createOwnerRight(beehiveTestCId, sub.toOption())
        }
    }

    @Test
    fun `it should not create an entity payload if one already exists`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE)).sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityService.createEntityPayload(
            ngsiLdEntity,
            jsonLdEntity,
            now,
        )

        entityService.createEntity(
            ngsiLdEntity,
            jsonLdEntity,
            sub,
        ).shouldFail { assertInstanceOf(AlreadyExistsException::class.java, it) }
    }

    @Test
    fun `it should allow to create an entity over a deleted one if authorized`() = runTest {
        coEvery { entityAttributeService.deleteAttributes(any(), any()) } returns Unit.right()
        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityAttributeService.createAttributes(any(), any(), any(), any(), any()) } returns Unit.right()
        coEvery { authorizationService.createOwnerRight(any(), any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
                .sampleDataToNgsiLdEntity()
                .shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            now
        )

        entityService.deleteEntityPayload(entity01Uri, ngsiLdDateTime())
            .shouldSucceedWith {
                assertEquals(entity01Uri, it.entityId)
                assertNotNull(it.payload)
            }

        entityService.createEntity(ngsiLdEntity, expandedEntity, null)
            .shouldSucceed()
    }

    @Test
    fun `it should not allow to create an entity over a deleted one if not authorized`() = runTest {
        coEvery { entityAttributeService.deleteAttributes(any(), any()) } returns Unit.right()
        coEvery {
            authorizationService.userCanAdminEntity(any(), any())
        } returns AccessDeniedException("Unauthorized").left()

        val (expandedEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
                .sampleDataToNgsiLdEntity()
                .shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            now
        )

        entityService.deleteEntityPayload(entity01Uri, ngsiLdDateTime())
            .shouldSucceedWith {
                assertEquals(entity01Uri, it.entityId)
                assertNotNull(it.payload)
            }

        entityService.createEntity(ngsiLdEntity, expandedEntity, null)
            .shouldFail { assertInstanceOf(AccessDeniedException::class.java, it) }
    }

    @Test
    fun `it should create the deleted representation of an entity when deleting it`() = runTest {
        val (expandedEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
                .sampleDataToNgsiLdEntity()
                .shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            now
        ).shouldSucceed()

        entityService.deleteEntityPayload(entity01Uri, ngsiLdDateTime()).shouldSucceed()

        entityQueryService.retrieve(entity01Uri)
            .shouldSucceedWith { entity ->
                val payload = entity.payload.deserializeAsMap()
                assertThat(payload)
                    .hasSize(2)
                    .containsKeys(JSONLD_ID, NGSILD_DELETED_AT_PROPERTY)
            }
    }

    @Test
    fun `it should merge an entity`() = runTest {
        coEvery { authorizationService.userCanCreateEntities(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            entityAttributeService.mergeAttributes(any(), any(), any(), any(), any(), any())
        } returns UpdateResult(
            listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.APPENDED)),
            emptyList()
        ).right()
        coEvery { entityAttributeService.getForEntity(any(), any(), any()) } returns emptyList()
        coEvery { authorizationService.createOwnerRight(any(), any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            sub
        ).shouldSucceed()

        val (jsonLdEntity, _) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()

        entityService.mergeEntity(
            beehiveTestCId,
            jsonLdEntity.getModifiableMembers(),
            now,
            sub
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt != null)
            }

        coVerify {
            authorizationService.userCanCreateEntities(sub.toOption())
            authorizationService.userCanUpdateEntity(beehiveTestCId, sub.toOption())
            entityAttributeService.createAttributes(
                any(),
                any(),
                emptyList(),
                any(),
                eq(sub)
            )
            entityAttributeService.mergeAttributes(
                eq(beehiveTestCId),
                any(),
                any(),
                any(),
                any(),
                eq(sub)
            )
            entityAttributeService.getForEntity(
                eq(beehiveTestCId),
                emptySet(),
                emptySet()
            )
            authorizationService.createOwnerRight(beehiveTestCId, sub.toOption())
        }
    }

    @Test
    fun `it should merge an entity with new types`() = runTest {
        coEvery { authorizationService.userCanCreateEntities(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            entityAttributeService.mergeAttributes(any(), any(), any(), any(), any(), any())
        } returns UpdateResult(
            listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.APPENDED)),
            emptyList()
        ).right()
        coEvery { entityAttributeService.getForEntity(any(), any(), any()) } returns emptyList()
        coEvery { authorizationService.createOwnerRight(any(), any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            sub
        ).shouldSucceed()

        val expandedAttributes = expandAttributes(
            loadSampleData("fragments/beehive_merge_entity_multiple_types.jsonld"),
            APIC_COMPOUND_CONTEXTS
        )

        entityService.mergeEntity(
            beehiveTestCId,
            expandedAttributes,
            now,
            sub
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.types.containsAll(setOf(BEEHIVE_TYPE, NGSILD_DEFAULT_VOCAB + "Distribution")))
            }
    }

    @Test
    fun `it should merge an entity with new types and scopes`() = runTest {
        coEvery { authorizationService.userCanCreateEntities(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
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
        coEvery { entityAttributeService.getForEntity(any(), any(), any()) } returns emptyList()
        coEvery { authorizationService.createOwnerRight(any(), any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            sub
        ).shouldSucceed()

        val expandedAttributes = expandAttributes(
            loadSampleData("fragments/beehive_merge_entity_multiple_types_and_scopes.jsonld"),
            APIC_COMPOUND_CONTEXTS
        )

        entityService.mergeEntity(
            beehiveTestCId,
            expandedAttributes,
            now,
            sub
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
        coEvery { authorizationService.userCanCreateEntities(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery { entityAttributeService.deleteAttributes(any(), any()) } returns Unit.right()
        coEvery { authorizationService.createOwnerRight(any(), any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity,
            sub
        ).shouldSucceed()

        val (newExpandedEntity, newNgsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()

        entityService.replaceEntity(
            beehiveTestCId,
            newNgsiLdEntity,
            newExpandedEntity,
            sub
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt != null)
                assertEquals(8, it.payload.deserializeAsMap().size)
            }

        coVerify {
            authorizationService.userCanCreateEntities(sub.toOption())
            authorizationService.userCanUpdateEntity(beehiveTestCId, sub.toOption())
            entityAttributeService.deleteAttributes(beehiveTestCId, any())
            entityAttributeService.createAttributes(
                any(),
                any(),
                emptyList(),
                any(),
                eq(sub)
            )
            authorizationService.createOwnerRight(beehiveTestCId, sub.toOption())
        }
    }

    @Test
    fun `it should replace an attribute`() = runTest {
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
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

        entityService.replaceAttribute(beehiveTestCId, expandedAttribute, sub)
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
    fun `it should remove the scopes from an entity`() = runTest {
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
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

    @Test
    fun `it should permanently delete an entity`() = runTest {
        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityAttributeService.permanentlyDeleteAttributes(any()) } returns Unit.right()
        coEvery { authorizationService.removeRightsOnEntity(any()) } returns Unit.right()

        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityService.permanentlyDeleteEntity(entity01Uri).shouldSucceed()

        entityQueryService.retrieve(entity01Uri)
            .shouldFail {
                assertInstanceOf(ResourceNotFoundException::class.java, it)
            }
    }

    @Test
    fun `it should permanently delete an attribute`() = runTest {
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery { entityAttributeService.checkEntityAndAttributeExistence(any(), any(), any()) } returns Unit.right()
        coEvery { entityAttributeService.permanentlyDeleteAttribute(any(), any(), any(), any()) } returns Unit.right()
        coEvery { entityAttributeService.getForEntity(any(), any(), any()) } returns emptyList()

        loadAndPrepareSampleData("beehive.jsonld")
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityService.permanentlyDeleteAttribute(beehiveTestCId, INCOMING_PROPERTY, null).shouldSucceed()

        coVerify {
            entityAttributeService.checkEntityAndAttributeExistence(beehiveTestCId, INCOMING_PROPERTY, null)
            entityAttributeService.permanentlyDeleteAttribute(beehiveTestCId, INCOMING_PROPERTY, null, false)
            entityAttributeService.getForEntity(beehiveTestCId, emptySet(), emptySet())
        }
    }

    @Test
    fun `it should return a ResourceNotFound error if trying to permanently delete an unknown attribute`() = runTest {
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityAttributeService.checkEntityAndAttributeExistence(any(), any(), any())
        } returns ResourceNotFoundException("Entity does not exist").left()

        loadAndPrepareSampleData("beehive.jsonld")
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityService.permanentlyDeleteAttribute(beehiveTestCId, OUTGOING_PROPERTY, null).shouldFail {
            assertInstanceOf(ResourceNotFoundException::class.java, it)
        }

        coVerify {
            entityAttributeService.checkEntityAndAttributeExistence(beehiveTestCId, OUTGOING_PROPERTY, null)
            listOf(
                entityAttributeService.permanentlyDeleteAttribute(beehiveTestCId, OUTGOING_PROPERTY, null, false),
                entityAttributeService.getForEntity(beehiveTestCId, emptySet(), emptySet())
            ) wasNot Called
        }
    }
}
