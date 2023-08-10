package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.OperationType.*
import com.egm.stellio.search.support.EMPTY_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.buildSapAttribute
import com.egm.stellio.search.util.deserializeAsMap
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_API_DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.just
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatList
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import java.util.stream.Stream

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class EntityPayloadServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val entity01Uri = "urn:ngsi-ld:Entity:01".toUri()
    private val entity02Uri = "urn:ngsi-ld:Entity:02".toUri()
    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val now = ngsiLdDateTime()

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should create an entity payload from string if none existed yet`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD
                ).shouldSucceed()
            }
    }

    @Test
    fun `it should create an entity payload from an NGSI-LD Entity if none existed yet`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityPayloadService.createEntityPayload(ngsiLdEntity, now, jsonLdEntity)
            .shouldSucceed()
    }

    @Test
    fun `it should create an entity payload from string with specificAccessPolicy`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD,
                    AUTH_READ
                )
            }
        loadMinimalEntity(entity02Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD,
                    SpecificAccessPolicy.AUTH_WRITE
                )
            }
        entityPayloadService.hasSpecificAccessPolicies(
            entity01Uri,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            entity01Uri,
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }

        entityPayloadService.hasSpecificAccessPolicies(
            entity02Uri,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            entity02Uri,
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should create an entity payload from an NGSI-LD Entity with specificAccessPolicy`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) =
            loadSampleData("beehive_with_sap.jsonld").sampleDataToNgsiLdEntity().shouldSucceedAndResult()

        entityPayloadService.createEntityPayload(
            ngsiLdEntity,
            now,
            jsonLdEntity
        )

        entityPayloadService.hasSpecificAccessPolicies(
            beehiveTestCId,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            beehiveTestCId,
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should only create an entity payload for a minimal entity`() = runTest {
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any(), any(), any())
        } returns Unit.right()

        val rawEntity = loadSampleData("beehive_minimal.jsonld")

        entityPayloadService.createEntity(
            rawEntity,
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(listOf(APIC_COMPOUND_CONTEXT), it.contexts)
            }

        coVerify {
            temporalEntityAttributeService.createEntityTemporalReferences(
                any(),
                any(),
                emptyList(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
        }
    }

    @Test
    fun `it should not create an entity payload if one already existed`() = runTest {
        val (_, ngsiLdEntity) =
            loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE)).sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityPayloadService.createEntityPayload(
            ngsiLdEntity,
            now,
            EMPTY_PAYLOAD,
        )

        assertThrows<DataIntegrityViolationException> {
            entityPayloadService.createEntityPayload(
                ngsiLdEntity,
                now,
                EMPTY_PAYLOAD,
            )
        }
    }

    @Test
    fun `it should merge an entity`() = runTest {
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            temporalEntityAttributeService.mergeEntityAttributes(any(), any(), any(), any(), any(), any())
        } returns UpdateResult(
            listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.APPENDED)),
            emptyList()
        ).right()
        coEvery {
            temporalEntityAttributeService.getForEntity(any(), any())
        } returns emptyList()

        val createEntityPayload = loadSampleData("beehive_minimal.jsonld")

        entityPayloadService.createEntity(
            createEntityPayload,
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val (jsonLdEntity, _) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()

        entityPayloadService.mergeEntity(
            beehiveTestCId,
            jsonLdEntity.getAttributes(),
            now,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt != null)
            }

        coVerify {
            temporalEntityAttributeService.createEntityTemporalReferences(
                any(),
                any(),
                emptyList(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
            temporalEntityAttributeService.mergeEntityAttributes(
                eq(beehiveTestCId),
                any(),
                any(),
                any(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
            temporalEntityAttributeService.getForEntity(
                eq(beehiveTestCId),
                emptySet()
            )
        }
    }

    @Test
    fun `it should merge an entity with new types`() = runTest {
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            temporalEntityAttributeService.mergeEntityAttributes(any(), any(), any(), any(), any(), any())
        } returns UpdateResult(
            listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.APPENDED)),
            emptyList()
        ).right()
        coEvery {
            temporalEntityAttributeService.getForEntity(any(), any())
        } returns emptyList()

        val createEntityPayload = loadSampleData("beehive_minimal.jsonld")

        entityPayloadService.createEntity(
            createEntityPayload,
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val expandedAttributes = expandAttributes(
            loadSampleData("fragments/beehive_merge_entity_multiple_types.jsonld"),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        entityPayloadService.mergeEntity(
            beehiveTestCId,
            expandedAttributes,
            now,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.types.containsAll(setOf(BEEHIVE_TYPE, NGSILD_DEFAULT_VOCAB + "Distribution")))
            }
    }

    @Test
    fun `it should merge an entity with new types and scopes`() = runTest {
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery {
            temporalEntityAttributeService.mergeEntityAttributes(any(), any(), any(), any(), any(), any())
        } returns UpdateResult(
            listOf(UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.APPENDED)),
            emptyList()
        ).right()
        coEvery {
            temporalEntityAttributeService.getForEntity(any(), any())
        } returns emptyList()

        val createEntityPayload = loadSampleData("beehive_minimal.jsonld")

        entityPayloadService.createEntity(
            createEntityPayload,
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val expandedAttributes = expandAttributes(
            loadSampleData("fragments/beehive_merge_entity_multiple_types_and_scopes.jsonld"),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        entityPayloadService.mergeEntity(
            beehiveTestCId,
            expandedAttributes,
            now,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.types.containsAll(setOf(BEEHIVE_TYPE, NGSILD_DEFAULT_VOCAB + "Distribution")))
                assertTrue(it.scopes.containsAll(setOf("/Nantes/BottiereChenaie", "/Agri/Beekeeping")))
            }
    }

    @Test
    fun `it should replace an entity payload if entity previously existed`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityPayloadService.createEntityPayload(ngsiLdEntity, now, jsonLdEntity).shouldSucceed()

        entityPayloadService.replaceEntityPayload(ngsiLdEntity, now, jsonLdEntity).shouldSucceed()
    }

    @Test
    fun `it should replace an entity`() = runTest {
        val beehiveURI = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any(), any(), any())
        } returns Unit.right()
        coEvery { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(any()) } just Runs

        val createEntityPayload = loadSampleData("beehive_minimal.jsonld")

        entityPayloadService.createEntity(
            createEntityPayload,
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()

        entityPayloadService.replaceEntity(
            beehiveURI,
            ngsiLdEntity,
            jsonLdEntity,
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt != null)
                assertEquals(8, it.payload.deserializeAsMap().size)
            }

        coVerify {
            temporalEntityAttributeService.deleteTemporalAttributesOfEntity(beehiveURI)
            temporalEntityAttributeService.createEntityTemporalReferences(
                any(),
                any(),
                emptyList(),
                any(),
                eq("0123456789-1234-5678-987654321")
            )
        }
    }

    @Test
    fun `it should retrieve an entity payload`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD,
                ).shouldSucceed()
            }

        entityPayloadService.retrieve(entity01Uri)
            .shouldSucceedWith {
                assertThat(it)
                    .hasFieldOrPropertyWithValue("entityId", entity01Uri)
                    .hasFieldOrPropertyWithValue("types", listOf(BEEHIVE_TYPE))
                    .hasFieldOrPropertyWithValue("createdAt", now)
                    .hasFieldOrPropertyWithValue("modifiedAt", null)
                    .hasFieldOrPropertyWithValue("contexts", listOf(NGSILD_CORE_CONTEXT))
                    .hasFieldOrPropertyWithValue("specificAccessPolicy", null)
            }
    }

    @Test
    fun `it should retrieve an entity payload with specificAccesPolicy`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD,
                    AUTH_READ
                )
            }

        entityPayloadService.retrieve(entity01Uri)
            .shouldSucceedWith {
                assertThat(it)
                    .hasFieldOrPropertyWithValue("specificAccessPolicy", AUTH_READ)
            }
    }

    @Test
    fun `it should retrieve a list of entity payloads`() = runTest {
        val expandedPayload = loadSampleData("beehive_expanded.jsonld")
        expandedPayload.sampleDataToNgsiLdEntity().map {
            entityPayloadService.createEntityPayload(
                it.second,
                now,
                expandedPayload
            )
        }

        val entityPayloads = entityPayloadService.retrieve(listOf(beehiveTestCId))
        assertEquals(1, entityPayloads.size)
        assertEquals(beehiveTestCId, entityPayloads[0].entityId)
        assertJsonPayloadsAreEqual(expandedPayload, entityPayloads[0].payload.asString())
    }

    @Test
    fun `it should check the existence or non-existence of an entity`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD
                )
            }

        entityPayloadService.checkEntityExistence(entity01Uri).shouldSucceed()
        entityPayloadService.checkEntityExistence(entity02Uri)
            .shouldFail { assert(it is ResourceNotFoundException) }
        entityPayloadService.checkEntityExistence(entity01Uri, true)
            .shouldFail { assert(it is AlreadyExistsException) }
        entityPayloadService.checkEntityExistence(entity02Uri, true).shouldSucceed()
    }

    @Test
    fun `it should filter existing entities from a list of ids`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD
                )
            }

        val existingEntities = entityPayloadService.filterExistingEntitiesAsIds(listOf(entity01Uri, entity02Uri))
        assertEquals(1, existingEntities.size)
        assertEquals(entity01Uri, existingEntities[0])
    }

    @Test
    fun `it should return an empty list if no ids are provided to the filter on existence`() = runTest {
        val existingEntities = entityPayloadService.filterExistingEntitiesAsIds(emptyList())
        assertTrue(existingEntities.isEmpty())
    }

    @Test
    fun `it should get the types of an entity`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE, APIARY_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD
                )
            }

        entityPayloadService.getTypes(entity01Uri)
            .shouldSucceedWith {
                assertThatList(it).containsAll(listOf(BEEHIVE_TYPE, APIARY_TYPE))
            }
    }

    @Test
    fun `it should return an error if trying to get the type of a non-existent entity`() = runTest {
        entityPayloadService.getTypes(entity01Uri)
            .shouldFail { assertTrue(it is ResourceNotFoundException) }
    }

    @Test
    fun `it should add a type to an entity`() = runTest {
        val expandedPayload = loadSampleData("beehive_expanded.jsonld")
        expandedPayload.sampleDataToNgsiLdEntity().map {
            entityPayloadService.createEntityPayload(
                it.second,
                now,
                expandedPayload
            )
        }

        entityPayloadService.updateTypes(beehiveTestCId, listOf(BEEHIVE_TYPE, APIARY_TYPE), ngsiLdDateTime(), false)
            .shouldSucceedWith {
                assertTrue(it.isSuccessful())
                assertEquals(1, it.updated.size)
                val updatedDetails = it.updated[0]
                assertEquals(JSONLD_TYPE, updatedDetails.attributeName)
                assertEquals(UpdateOperationResult.APPENDED, updatedDetails.updateOperationResult)
            }

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(listOf(BEEHIVE_TYPE, APIARY_TYPE), it.types)
                assertEquals(
                    listOf(BEEHIVE_TYPE, APIARY_TYPE),
                    it.payload.asString().deserializeExpandedPayload()[JSONLD_TYPE]
                )
            }
    }

    @Test
    fun `it should not add a type if existing types are not in the list of types to add`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD
                )
            }

        entityPayloadService.updateTypes(entity01Uri, listOf(APIARY_TYPE), ngsiLdDateTime(), false)
            .shouldSucceedWith {
                assertFalse(it.isSuccessful())
                assertEquals(1, it.notUpdated.size)
                val notUpdatedDetails = it.notUpdated[0]
                assertEquals(JSONLD_TYPE, notUpdatedDetails.attributeName)
                assertEquals(
                    "A type cannot be removed from an entity: [$BEEHIVE_TYPE] have been removed",
                    notUpdatedDetails.reason
                )
            }
    }

    @Suppress("unused")
    private fun generateScopes(): Stream<Arguments> =
        Stream.of(
            Arguments.of(
                "beehive_with_scope.jsonld",
                listOf("/B", "/C"),
                APPEND_ATTRIBUTES,
                listOf("/A", "/B", "/C")
            ),
            Arguments.of(
                "beehive_with_scope.jsonld",
                listOf("/B", "/C"),
                REPLACE_ATTRIBUTE,
                listOf("/B", "/C")
            ),
            Arguments.of(
                "beehive_with_scope.jsonld",
                listOf("/B", "/C"),
                UPDATE_ATTRIBUTES,
                listOf("/B", "/C")
            ),
            Arguments.of(
                "beehive_minimal.jsonld",
                listOf("/B", "/C"),
                UPDATE_ATTRIBUTES,
                emptyList<String>()
            )
        )

    @ParameterizedTest
    @MethodSource("generateScopes")
    fun `it shoud update scopes as requested by the type of the operation`(
        initialEntity: String,
        inputScopes: List<String>,
        operationType: OperationType,
        expectedScopes: List<String>
    ) = runTest {
        loadSampleData(initialEntity)
            .sampleDataToNgsiLdEntity()
            .map { entityPayloadService.createEntityPayload(it.second, now, it.first) }

        val expandedAttributes = expandAttributes(
            """
            { 
                "scope": [${inputScopes.joinToString(",") { "\"$it\"" } }]
            }
            """.trimIndent(),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        entityPayloadService.updateScopes(
            beehiveTestCId,
            expandedAttributes[NGSILD_SCOPE_PROPERTY]!!,
            ngsiLdDateTime(),
            operationType
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(expectedScopes, it.scopes)
                val scopesInEntity = (it.payload.deserializeAsMap() as ExpandedAttributeInstance).getScopes()
                assertEquals(expectedScopes, scopesInEntity)
            }
    }

    @Test
    fun `it should upsert an entity payload if one already existed`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD
                )
            }

        entityPayloadService.upsertEntityPayload(entity01Uri, EMPTY_PAYLOAD)
            .shouldSucceed()
    }

    @Test
    fun `it should update a specific access policy for a temporal entity`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD,
                    AUTH_READ
                )
            }
        loadMinimalEntity(entity02Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD,
                    AUTH_READ
                )
            }

        entityPayloadService.updateSpecificAccessPolicy(
            entity01Uri,
            buildSapAttribute(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceed()

        entityPayloadService.hasSpecificAccessPolicies(
            entity01Uri,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            entity01Uri,
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertTrue(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            entity02Uri,
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should delete an entity payload`() = runTest {
        coEvery { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(any()) } just Runs

        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD
                )
            }

        val deleteResult = entityPayloadService.deleteEntity(entity01Uri)
        assertTrue(deleteResult.isRight())

        // if correctly deleted, we should be able to create a new one
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD
                ).shouldSucceed()
            }
    }

    @Test
    fun `it should remove a specific access policy from a entity payload`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_TYPE))
            .sampleDataToNgsiLdEntity()
            .map {
                entityPayloadService.createEntityPayload(
                    it.second,
                    now,
                    EMPTY_PAYLOAD,
                    AUTH_READ
                )
            }

        entityPayloadService.hasSpecificAccessPolicies(
            entity01Uri,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityPayloadService.removeSpecificAccessPolicy(entity01Uri).shouldSucceed()
        entityPayloadService.hasSpecificAccessPolicies(
            entity01Uri,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should return a 400 if the payload contains a multi-instance property`() = runTest {
        val requestPayload =
            """
            [{
                "type": "Property",
                "value": "AUTH_READ"
            },{
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Dataset:01",
                "value": "AUTH_WRITE"
            }]
            """.trimIndent()

        val ngsiLdAttribute =
            expandAttribute(AUTH_TERM_SAP, requestPayload, AUTHORIZATION_API_DEFAULT_CONTEXTS).toNgsiLdAttribute()
                .shouldSucceedAndResult()

        entityPayloadService.getSpecificAccessPolicy(ngsiLdAttribute)
            .shouldFail {
                assertThat(it)
                    .isInstanceOf(BadRequestDataException::class.java)
                    .hasFieldOrPropertyWithValue("message", "Payload must contain a single attribute instance")
            }
    }

    @Test
    fun `it should ignore properties that are not part of the payload when setting a specific access policy`() =
        runTest {
            val requestPayload =
                """
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "value": "AUTH_READ"
                }
                """.trimIndent()

            val ngsiLdAttribute =
                expandAttribute(AUTH_TERM_SAP, requestPayload, AUTHORIZATION_API_DEFAULT_CONTEXTS).toNgsiLdAttribute()
                    .shouldSucceedAndResult()

            entityPayloadService.getSpecificAccessPolicy(ngsiLdAttribute)
                .shouldSucceedWith { assertEquals(AUTH_READ, it) }
        }

    @Test
    fun `it should return a 400 if the value is not one of the supported`() = runTest {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "someValue"
            }
            """.trimIndent()

        val ngsiLdAttribute =
            expandAttribute(AUTH_TERM_SAP, requestPayload, AUTHORIZATION_API_DEFAULT_CONTEXTS).toNgsiLdAttribute()
                .shouldSucceedAndResult()

        val expectedMessage =
            "Value must be one of AUTH_READ or AUTH_WRITE " +
                "(No enum constant com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.someValue)"

        entityPayloadService.getSpecificAccessPolicy(ngsiLdAttribute)
            .shouldFail {
                assertThat(it)
                    .isInstanceOf(BadRequestDataException::class.java)
                    .hasFieldOrPropertyWithValue("message", expectedMessage)
            }
    }

    @Test
    fun `it should return a 400 if the provided attribute is a relationship`() = runTest {
        val requestPayload =
            """
            {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Entity:01"
            }
            """.trimIndent()

        val ngsiLdAttribute =
            expandAttribute(AUTH_TERM_SAP, requestPayload, AUTHORIZATION_API_DEFAULT_CONTEXTS).toNgsiLdAttribute()
                .shouldSucceedAndResult()

        entityPayloadService.getSpecificAccessPolicy(ngsiLdAttribute)
            .shouldFail {
                assertThat(it)
                    .isInstanceOf(BadRequestDataException::class.java)
                    .hasFieldOrPropertyWithValue("message", "Payload must be a property")
            }
    }

    @Test
    fun `it should return nothing when specific access policy list is empty`() = runTest {
        entityPayloadService.hasSpecificAccessPolicies(entity01Uri, emptyList())
            .shouldSucceedWith { assertFalse(it) }
    }
}
