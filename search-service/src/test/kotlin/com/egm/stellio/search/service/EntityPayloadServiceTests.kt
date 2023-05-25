package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.UpdateOperationResult
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
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class EntityPayloadServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var entityAttributeCleanerService: EntityAttributeCleanerService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val entity01Uri = "urn:ngsi-ld:Entity:01".toUri()
    private val entity02Uri = "urn:ngsi-ld:Entity:02".toUri()
    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val now = Instant.now().atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should create an entity payload from string if none existed yet`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT)
        ).shouldSucceed()
    }

    @Test
    fun `it should create an entity payload from an NGSI-LD Entity if none existed yet`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityPayloadService.createEntityPayload(ngsiLdEntity, now, jsonLdEntity)
            .shouldSucceed()
    }

    @Test
    fun `it should create an entity payload from string with specificAccessPolicy`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            AUTH_READ
        )
        entityPayloadService.createEntityPayload(
            entity02Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            SpecificAccessPolicy.AUTH_WRITE
        )
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
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT)
        )

        assertThrows<DataIntegrityViolationException> {
            entityPayloadService.createEntityPayload(
                entity01Uri,
                listOf(BEEHIVE_TYPE),
                now,
                EMPTY_PAYLOAD,
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `it should replace an entity payload from an NGSI-LD Entity if it existed`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityPayloadService.createEntityPayload(ngsiLdEntity, now, jsonLdEntity)
            .shouldSucceed()

        entityPayloadService.replaceEntityPayload(ngsiLdEntity, now, jsonLdEntity)
            .shouldSucceed()
    }

    @Test
    fun `it should replace an entity`() = runTest {
        val beehiveURI = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        coEvery {
            temporalEntityAttributeService.createEntityTemporalReferences(any(), any(), any(), any(), any())
        } returns Unit.right()

        val createEntityPayload = loadSampleData("beehive_minimal.jsonld")

        entityPayloadService.createEntity(
            createEntityPayload,
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt == null)
                assertEquals(3, it.payload.deserializeAsMap().size)
            }

        val replaceEntityPayload = loadSampleData("beehive.jsonld")

        entityPayloadService.replaceEntity(
            beehiveURI,
            replaceEntityPayload,
            listOf(APIC_COMPOUND_CONTEXT),
            "0123456789-1234-5678-987654321"
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertTrue(it.modifiedAt != null)
                assertEquals(8, it.payload.deserializeAsMap().size)
            }

        coVerify {
            entityAttributeCleanerService.deleteEntityAttributes(
                beehiveURI
            )
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
    fun `it should retrieve an entity payload`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT)
        ).shouldSucceed()

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
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            AUTH_READ
        )

        entityPayloadService.retrieve(entity01Uri)
            .shouldSucceedWith {
                assertThat(it)
                    .hasFieldOrPropertyWithValue("specificAccessPolicy", AUTH_READ)
            }
    }

    @Test
    fun `it should retrieve a list of entity payloads`() = runTest {
        val expandedPayload = loadSampleData("beehive_expanded.jsonld")
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            expandedPayload,
            listOf(APIC_COMPOUND_CONTEXT),
            null
        )

        val entityPayloads = entityPayloadService.retrieve(listOf(entity01Uri))
        assertEquals(1, entityPayloads.size)
        assertEquals(entity01Uri, entityPayloads[0].entityId)
        assertJsonPayloadsAreEqual(expandedPayload, entityPayloads[0].payload.asString())
    }

    @Test
    fun `it should check the existence or non-existence of an entity`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(APIC_COMPOUND_CONTEXT),
            null
        )

        entityPayloadService.checkEntityExistence(entity01Uri).shouldSucceed()
        entityPayloadService.checkEntityExistence(entity02Uri)
            .shouldFail { assert(it is ResourceNotFoundException) }
        entityPayloadService.checkEntityExistence(entity01Uri, true)
            .shouldFail { assert(it is AlreadyExistsException) }
        entityPayloadService.checkEntityExistence(entity02Uri, true).shouldSucceed()
    }

    @Test
    fun `it should filter existing entities from a list of ids`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(APIC_COMPOUND_CONTEXT),
            null
        )

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
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE, APIARY_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(APIC_COMPOUND_CONTEXT),
            null
        )

        entityPayloadService.getTypes(entity01Uri)
            .shouldSucceedWith {
                assertEquals(listOf(BEEHIVE_TYPE, APIARY_TYPE), it)
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
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            expandedPayload,
            listOf(APIC_COMPOUND_CONTEXT),
            null
        )

        entityPayloadService.updateTypes(entity01Uri, listOf(BEEHIVE_TYPE, APIARY_TYPE), false)
            .shouldSucceedWith {
                assertTrue(it.isSuccessful())
                assertEquals(1, it.updated.size)
                val updatedDetails = it.updated[0]
                assertEquals(JSONLD_TYPE, updatedDetails.attributeName)
                assertEquals(UpdateOperationResult.APPENDED, updatedDetails.updateOperationResult)
            }

        entityPayloadService.retrieve(entity01Uri)
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
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(APIC_COMPOUND_CONTEXT),
            null
        )

        entityPayloadService.updateTypes(entity01Uri, listOf(APIARY_TYPE), false)
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

    @Test
    fun `it should upsert an entity payload if one already existed`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT)
        )

        entityPayloadService.upsertEntityPayload(entity01Uri, EMPTY_PAYLOAD)
            .shouldSucceed()
    }

    @Test
    fun `it should update a specific access policy for a temporal entity`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            AUTH_READ
        )
        entityPayloadService.createEntityPayload(
            entity02Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            AUTH_READ
        )

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
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT)
        )

        val deleteResult = entityPayloadService.deleteEntityPayload(entity01Uri)
        assertTrue(deleteResult.isRight())

        // if correctly deleted, we should be able to create a new one
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT)
        ).shouldSucceed()
    }

    @Test
    fun `it should remove a specific access policy from a entity payload`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            AUTH_READ
        )

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
