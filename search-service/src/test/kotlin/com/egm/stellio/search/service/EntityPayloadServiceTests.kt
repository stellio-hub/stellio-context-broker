package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.UpdateOperationResult
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.util.buildSapAttribute
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_API_DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
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

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val entity01Uri = "urn:ngsi-ld:Entity:01".toUri()
    private val entity02Uri = "urn:ngsi-ld:Entity:02".toUri()
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
            entity01Uri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        ).shouldSucceed()
    }

    @Test
    fun `it should create an entity payload from an NGSI-LD Entity if none existed yet`() = runTest {
        val (jsonLdEntity, ngsiLdEntity) = loadSampleData().sampleDataToNgsiLdEntity()
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
        val (jsonLdEntity, ngsiLdEntity) = loadSampleData("beehive_with_sap.jsonld").sampleDataToNgsiLdEntity()

        entityPayloadService.createEntityPayload(
            ngsiLdEntity,
            now,
            jsonLdEntity
        )

        entityPayloadService.hasSpecificAccessPolicies(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should not create an entity payload if one already existed`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        )

        assertThrows<DataIntegrityViolationException> {
            entityPayloadService.createEntityPayload(
                entity01Uri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `it should retrieve an entity payload`() = runTest {
        entityPayloadService.createEntityPayload(
            entity01Uri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
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
        assertJsonPayloadsAreEqual(expandedPayload, entityPayloads[0].entityPayload)
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
                    it.entityPayload.deserializeExpandedPayload()[JSONLD_TYPE]
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
            entity01Uri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
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
            entity01Uri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        )

        val deleteResult = entityPayloadService.deleteEntityPayload(entity01Uri)
        assertTrue(deleteResult.isRight())

        // if correctly deleted, we should be able to create a new one
        entityPayloadService.createEntityPayload(
            entity01Uri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
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
    fun `it should return a 400 if the payload contains a multi-instance property`() {
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
        val ngsiLdAttributes = parseToNgsiLdAttributes(
            expandJsonLdFragment(AUTH_TERM_SAP, requestPayload, AUTHORIZATION_API_DEFAULT_CONTEXTS)
        )

        entityPayloadService.getSpecificAccessPolicy(ngsiLdAttributes[0])
            .shouldFail {
                assertThat(it)
                    .isInstanceOf(BadRequestDataException::class.java)
                    .hasFieldOrPropertyWithValue("message", "Payload must contain a single attribute instance")
            }
    }

    @Test
    fun `it should ignore properties that are not part of the payload when setting a specific access policy`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Dataset:01",
                "value": "AUTH_READ"
            }
            """.trimIndent()

        val ngsiLdAttributes = parseToNgsiLdAttributes(
            expandJsonLdFragment(AUTH_TERM_SAP, requestPayload, AUTHORIZATION_API_DEFAULT_CONTEXTS)
        )

        entityPayloadService.getSpecificAccessPolicy(ngsiLdAttributes[0])
            .shouldSucceedWith { assertEquals(AUTH_READ, it) }
    }

    @Test
    fun `it should return a 400 if the value is not one of the supported`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "someValue"
            }
            """.trimIndent()

        val ngsiLdAttributes = parseToNgsiLdAttributes(
            expandJsonLdFragment(AUTH_TERM_SAP, requestPayload, AUTHORIZATION_API_DEFAULT_CONTEXTS)
        )

        val expectedMessage =
            "Value must be one of AUTH_READ or AUTH_WRITE " +
                "(No enum constant com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.someValue)"

        entityPayloadService.getSpecificAccessPolicy(ngsiLdAttributes[0])
            .shouldFail {
                assertThat(it)
                    .isInstanceOf(BadRequestDataException::class.java)
                    .hasFieldOrPropertyWithValue("message", expectedMessage)
            }
    }

    @Test
    fun `it should return a 400 if the provided attribute is a relationship`() {
        val requestPayload =
            """
            {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Entity:01"
            }
            """.trimIndent()

        val ngsiLdAttributes = parseToNgsiLdAttributes(
            expandJsonLdFragment(AUTH_TERM_SAP, requestPayload, AUTHORIZATION_API_DEFAULT_CONTEXTS)
        )

        entityPayloadService.getSpecificAccessPolicy(ngsiLdAttributes[0])
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
