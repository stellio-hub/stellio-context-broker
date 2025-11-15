package com.egm.stellio.search.entity.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.authorization.subject.USER_UUID
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.gimmeSucceededAttributeOperationResult
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.CompactedAttributeInstances
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.model.NGSILD_DELETED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_SCOPE_IRI
import com.egm.stellio.shared.model.NGSILD_TITLE_TERM
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.NAME_TERM
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.loadAndExpandDeletedEntity
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
class EntityServiceTests : WithTimescaleContainer, WithKafkaContainer() {
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
    private val now = ngsiLdDateTime()

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
    }

    @Test
    fun `it should create an entity payload from string if none existed yet`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
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

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertNotNull(it.createdAt)
                assertNotNull(it.modifiedAt)
                assertEquals(it.createdAt, it.modifiedAt)
            }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `it should only create an entity payload for a minimal entity`() = runTest {
        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery { entityEventService.publishEntityCreateEvent(any(), any()) } returns Job()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()
        coEvery { authorizationService.createOwnerRight(any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(beehiveTestCId, it.entityId)
                assertEquals(listOf(BEEHIVE_IRI), it.types)
            }

        coVerify {
            authorizationService.userCanCreateEntities()
            entityAttributeService.createAttributes(
                any(),
                any(),
                emptyList(),
                any()
            )
            entityEventService.publishEntityCreateEvent(
                eq(USER_UUID),
                any()
            )
            authorizationService.createOwnerRight(beehiveTestCId)
        }
    }

    @Test
    fun `it should not create an entity payload if one already exists`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI)).sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityService.createEntityPayload(
            ngsiLdEntity,
            jsonLdEntity,
            now,
        )

        entityService.createEntity(
            ngsiLdEntity,
            jsonLdEntity
        ).shouldFail { assertInstanceOf(AlreadyExistsException::class.java, it) }
    }

    @Test
    fun `it should allow to create an entity over a deleted one if authorized`() = runTest {
        coEvery {
            entityAttributeService.deleteAttributes(any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()

        val (expandedEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
                .sampleDataToNgsiLdEntity()
                .shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            now
        )

        entityService.deleteEntityPayload(entity01Uri, ngsiLdDateTime(), loadAndExpandDeletedEntity(entity01Uri))
            .shouldSucceedWith {
                assertEquals(entity01Uri, it.entityId)
                assertNotNull(it.payload)
            }

        entityService.createEntity(ngsiLdEntity, expandedEntity)
            .shouldSucceed()
    }

    @Test
    fun `it should not allow to create an entity over a deleted one if not authorized`() = runTest {
        coEvery {
            entityAttributeService.deleteAttributes(any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()
        coEvery {
            authorizationService.userCanAdminEntity(any())
        } returns AccessDeniedException("Unauthorized").left()

        val (expandedEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
                .sampleDataToNgsiLdEntity()
                .shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            now
        )

        entityService.deleteEntityPayload(entity01Uri, ngsiLdDateTime(), loadAndExpandDeletedEntity(entity01Uri))
            .shouldSucceedWith {
                assertEquals(entity01Uri, it.entityId)
                assertNotNull(it.payload)
            }

        entityService.createEntity(ngsiLdEntity, expandedEntity)
            .shouldFail { assertInstanceOf(AccessDeniedException::class.java, it) }
    }

    @Test
    fun `it should create the deleted representation of an entity when deleting it`() = runTest {
        val (expandedEntity, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
                .sampleDataToNgsiLdEntity()
                .shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            now
        ).shouldSucceed()

        entityService.deleteEntityPayload(entity01Uri, ngsiLdDateTime(), loadAndExpandDeletedEntity(entity01Uri))
            .shouldSucceed()

        entityQueryService.retrieve(entity01Uri, false)
            .shouldSucceedWith { entity ->
                val payload = entity.payload.deserializeAsMap()
                assertThat(payload)
                    .hasSize(2)
                    .containsKeys(JSONLD_ID_KW, NGSILD_DELETED_AT_IRI)
            }
    }

    @Test
    fun `it should merge an entity`() = runTest {
        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()
        coEvery {
            entityAttributeService.mergeAttributes(any(), any(), any(), any(), any())
        } returns listOf(
            SucceededAttributeOperationResult(INCOMING_IRI, null, OperationStatus.CREATED, emptyMap()),
        ).right()
        coEvery { entityAttributeService.getAllForEntity(any()) } returns emptyList()
        coEvery { authorizationService.createOwnerRight(any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity
        ).shouldSucceed()

        val (jsonLdEntity, _) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()

        entityService.mergeEntity(
            beehiveTestCId,
            jsonLdEntity.getModifiableMembers(),
            now
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt > it.createdAt)
            }

        coVerify {
            authorizationService.userCanCreateEntities()
            authorizationService.userCanUpdateEntity(beehiveTestCId)
            entityAttributeService.createAttributes(
                any(),
                any(),
                emptyList(),
                any()
            )
            entityAttributeService.mergeAttributes(
                eq(beehiveTestCId),
                any(),
                any(),
                any(),
                any()
            )
            entityAttributeService.getAllForEntity(
                eq(beehiveTestCId)
            )
            authorizationService.createOwnerRight(beehiveTestCId)
        }
    }

    @Test
    fun `it should merge an entity with new types`() = runTest {
        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()
        coEvery {
            entityAttributeService.mergeAttributes(any(), any(), any(), any(), any())
        } returns listOf(
            SucceededAttributeOperationResult(INCOMING_IRI, null, OperationStatus.CREATED, emptyMap())
        ).right()
        coEvery { entityAttributeService.getAllForEntity(any()) } returns emptyList()
        coEvery { authorizationService.createOwnerRight(any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity
        ).shouldSucceed()

        val expandedAttributes = expandAttributes(
            loadSampleData("fragments/beehive_merge_entity_multiple_types.jsonld"),
            APIC_COMPOUND_CONTEXTS
        )

        entityService.mergeEntity(
            beehiveTestCId,
            expandedAttributes,
            now
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.types.containsAll(setOf(BEEHIVE_IRI, NGSILD_DEFAULT_VOCAB + "Distribution")))
            }
    }

    @Test
    fun `it should merge an entity with new types and scopes`() = runTest {
        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()
        coEvery {
            entityAttributeService.mergeAttributes(any(), any(), any(), any(), any())
        } returns listOf(
            SucceededAttributeOperationResult(INCOMING_IRI, null, OperationStatus.CREATED, emptyMap())
        ).right()
        coEvery { entityAttributeService.getAllForEntity(any()) } returns emptyList()
        coEvery { authorizationService.createOwnerRight(any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity
        ).shouldSucceed()

        val expandedAttributes = expandAttributes(
            loadSampleData("fragments/beehive_merge_entity_multiple_types_and_scopes.jsonld"),
            APIC_COMPOUND_CONTEXTS
        )

        entityService.mergeEntity(
            beehiveTestCId,
            expandedAttributes,
            now
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.types.containsAll(setOf(BEEHIVE_IRI, NGSILD_DEFAULT_VOCAB + "Distribution")))
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
        // called when creating the initial entity
        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery {
            entityAttributeService.createAttributes(any(), any(), any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()
        coEvery { authorizationService.createOwnerRight(any()) } returns Unit.right()
        // called when replacing the initial entity
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery {
            entityAttributeService.appendAttributes(any(), any(), any(), any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()
        coEvery {
            entityAttributeService.deleteAttribute(any(), any(), any(), any(), any())
        } returns emptyList<SucceededAttributeOperationResult>().right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_for_replacement.jsonld").shouldSucceedAndResult()

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity
        ).shouldSucceed()

        val (newExpandedEntity, newNgsiLdEntity) =
            loadAndPrepareSampleData("beehive_replacement.jsonld").shouldSucceedAndResult()

        entityService.replaceEntity(
            beehiveTestCId,
            newNgsiLdEntity,
            newExpandedEntity
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt > it.createdAt)
                val compactedEntity = compactEntity(it.toExpandedEntity(), APIC_COMPOUND_CONTEXTS)
                assertThat(compactedEntity)
                    .hasSize(7)
                    .containsKey(NGSILD_TITLE_TERM)
                    .doesNotContainKey(NAME_TERM)
                val incomingProperty = compactedEntity[INCOMING_TERM] as CompactedAttributeInstances
                assertThat(incomingProperty)
                    .hasSize(2)
                    .allMatch {
                        it[NGSILD_VALUE_TERM] == 5678
                    }
            }

        coVerify {
            authorizationService.userCanUpdateEntity(beehiveTestCId)
            entityAttributeService.deleteAttribute(beehiveTestCId, NAME_IRI, null, false, any())
            entityAttributeService.appendAttributes(
                beehiveTestCId,
                any(),
                any(),
                false,
                any()
            )
        }
    }

    @Test
    fun `it should replace an attribute`() = runTest {
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery { entityAttributeService.getAllForEntity(any()) } returns emptyList()
        coEvery {
            entityAttributeService.replaceAttribute(any(), any(), any(), any())
        } returns SucceededAttributeOperationResult(
            attributeName = INCOMING_IRI,
            operationStatus = OperationStatus.UPDATED,
            newExpandedValue = emptyMap()
        ).right()

        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityService.createEntityPayload(ngsiLdEntity, jsonLdEntity, now).shouldSucceed()

        val expandedAttribute = expandAttribute(
            loadSampleData("fragments/beehive_new_incoming_property.json"),
            APIC_COMPOUND_CONTEXTS
        )

        entityService.replaceAttribute(beehiveTestCId, expandedAttribute)
            .shouldSucceedWith {
                it.updated.size == 1 &&
                    it.notUpdated.isEmpty() &&
                    it.updated[0] == INCOMING_IRI
            }
    }

    @Test
    fun `it should add a type to an entity`() = runTest {
        val entityPayload = loadSampleData("beehive.jsonld")
        entityPayload.sampleDataToNgsiLdEntity().map {
            entityService.createEntityPayload(
                it.second,
                it.first,
                now
            )
        }

        entityService.updateTypes(beehiveTestCId, listOf(BEEHIVE_IRI, APIARY_IRI), ngsiLdDateTime(), false)
            .shouldSucceedWith {
                assertInstanceOf(SucceededAttributeOperationResult::class.java, it)
                assertEquals(JSONLD_TYPE_KW, it.attributeName)
                assertEquals(OperationStatus.CREATED, it.operationStatus)
            }

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(listOf(BEEHIVE_IRI, APIARY_IRI), it.types)
                assertEquals(
                    listOf(BEEHIVE_IRI, APIARY_IRI),
                    it.payload.asString().deserializeExpandedPayload()[JSONLD_TYPE_KW]
                )
            }
    }

    @Test
    fun `it should add a type to an entity even if existing types are not in the list of types to add`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }
        entityService.updateTypes(entity01Uri, listOf(APIARY_IRI), ngsiLdDateTime(), false)
            .shouldSucceed()

        entityQueryService.retrieve(entity01Uri)
            .shouldSucceedWith {
                assertEquals(listOf(BEEHIVE_IRI, APIARY_IRI), it.types)
                assertEquals(
                    listOf(BEEHIVE_IRI, APIARY_IRI),
                    it.payload.asString().deserializeExpandedPayload()[JSONLD_TYPE_KW]
                )
            }
    }

    @Test
    fun `it should remove the scopes from an entity`() = runTest {
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery {
            entityAttributeService.addOrReplaceAttribute(any(), any(), any(), any(), any())
        } returns gimmeSucceededAttributeOperationResult().right()
        coEvery {
            entityAttributeService.getAllForEntity(any())
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

        entityService.deleteAttribute(beehiveTestCId, NGSILD_SCOPE_IRI, null)
            .shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertNull(it.scopes)
            }
    }

    @Test
    fun `it should permanently delete an entity`() = runTest {
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAttributeService.permanentlyDeleteAttributes(any()) } returns Unit.right()
        coEvery { authorizationService.removeRightsOnEntity(any()) } returns Unit.right()

        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
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
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery {
            entityAttributeService.checkEntityAndAttributeExistence(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery { entityAttributeService.permanentlyDeleteAttribute(any(), any(), any(), any()) } returns Unit.right()
        coEvery { entityAttributeService.getAllForEntity(any()) } returns emptyList()

        loadAndPrepareSampleData("beehive.jsonld")
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityService.permanentlyDeleteAttribute(beehiveTestCId, INCOMING_IRI, null).shouldSucceed()

        coVerify {
            entityAttributeService.checkEntityAndAttributeExistence(beehiveTestCId, INCOMING_IRI, null, false, false)
            entityAttributeService.permanentlyDeleteAttribute(beehiveTestCId, INCOMING_IRI, null, false)
            entityAttributeService.getAllForEntity(beehiveTestCId)
        }
    }

    @Test
    fun `it should return a ResourceNotFound error if trying to permanently delete an unknown attribute`() = runTest {
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery {
            entityAttributeService.checkEntityAndAttributeExistence(any(), any(), any(), any(), any())
        } returns ResourceNotFoundException("Entity does not exist").left()

        loadAndPrepareSampleData("beehive.jsonld")
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityService.permanentlyDeleteAttribute(beehiveTestCId, OUTGOING_IRI, null).shouldFail {
            assertInstanceOf(ResourceNotFoundException::class.java, it)
        }

        coVerify {
            entityAttributeService.checkEntityAndAttributeExistence(beehiveTestCId, OUTGOING_IRI, null, false, false)
        }
        coVerify(exactly = 0) {
            entityAttributeService.permanentlyDeleteAttribute(beehiveTestCId, OUTGOING_IRI, null, false)
            entityAttributeService.getAllForEntity(beehiveTestCId)
        }
    }
}
