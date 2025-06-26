package com.egm.stellio.search.entity.service

import arrow.core.right
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.buildDefaultQueryParams
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.AUTHZ_TEST_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadAndPrepareSampleData
import com.egm.stellio.shared.util.loadMinimalEntity
import com.egm.stellio.shared.util.loadMinimalEntityWithSap
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.sampleDataToNgsiLdEntity
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
class EntityQueryServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var entityQueryService: EntityQueryService

    @Autowired
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val entity01Uri = "urn:ngsi-ld:Entity:01".toUri()
    private val entity02Uri = "urn:ngsi-ld:Entity:02".toUri()
    private val now = ngsiLdDateTime()

    @AfterEach
    fun deleteEntities() {
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
        r2dbcEntityTemplate.delete<Attribute>().from("temporal_entity_attribute").all().block()
    }

    @Test
    fun `it should return a JSON-LD entity when querying by id`() = runTest {
        coEvery { authorizationService.userCanReadEntity(any(), any()) } returns Unit.right()

        loadAndPrepareSampleData("beehive.jsonld")
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now,
                ).shouldSucceed()
            }

        entityQueryService.queryEntity(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(beehiveTestCId, it.id)
                assertEquals(listOf(BEEHIVE_IRI), it.types)
                assertEquals(8, it.members.size)
            }
    }

    @Test
    fun `it should return an API exception if no entity exists with the given id`() = runTest {
        entityQueryService.queryEntity(entity01Uri)
            .shouldFail {
                assertTrue(it is ResourceNotFoundException)
            }
    }

    @Test
    fun `it should return a list of JSON-LD entities when querying entities`() = runTest {
        coEvery { authorizationService.userCanCreateEntities(any()) } returns Unit.right()
        coEvery { authorizationService.createOwnerRight(any(), any()) } returns Unit.right()
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }

        loadAndPrepareSampleData("beehive.jsonld")
            .map {
                entityService.createEntity(
                    it.second,
                    it.first
                ).shouldSucceed()
            }

        entityQueryService.queryEntities(buildDefaultQueryParams().copy(ids = setOf(beehiveTestCId)))
            .shouldSucceedWith {
                assertEquals(1, it.second)
                assertEquals(beehiveTestCId, it.first[0].id)
                assertEquals(listOf(BEEHIVE_IRI), it.first[0].types)
                assertEquals(8, it.first[0].members.size)
            }
    }

    @Test
    fun `it should return an empty list if no entity matched the query`() = runTest {
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }

        entityQueryService.queryEntities(buildDefaultQueryParams().copy(ids = setOf(entity01Uri)))
            .shouldSucceedWith {
                assertEquals(0, it.second)
                assertTrue(it.first.isEmpty())
            }
    }

    @Test
    fun `it should retrieve an entity payload`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now,
                ).shouldSucceed()
            }

        entityQueryService.retrieve(entity01Uri)
            .shouldSucceedWith {
                assertThat(it)
                    .hasFieldOrPropertyWithValue("entityId", entity01Uri)
                    .hasFieldOrPropertyWithValue("types", listOf(BEEHIVE_IRI))
                    .hasFieldOrPropertyWithValue("createdAt", now)
                    .hasFieldOrPropertyWithValue("modifiedAt", now)
                    .hasFieldOrPropertyWithValue("specificAccessPolicy", null)
            }
    }

    @Test
    fun `it should retrieve an entity payload with specificAccesPolicy`() = runTest {
        loadMinimalEntityWithSap(entity01Uri, setOf(BEEHIVE_IRI), AUTH_READ, AUTHZ_TEST_COMPOUND_CONTEXTS)
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityQueryService.retrieve(entity01Uri)
            .shouldSucceedWith {
                assertThat(it)
                    .hasFieldOrPropertyWithValue("specificAccessPolicy", AUTH_READ)
            }
    }

    @Test
    fun `it should retrieve a list of entity payloads`() = runTest {
        val entityPayload = loadSampleData("beehive.jsonld")
        entityPayload.sampleDataToNgsiLdEntity().map {
            entityService.createEntityPayload(
                it.second,
                it.first,
                ZonedDateTime.parse("2023-08-20T15:44:10.381090Z")
            )
        }

        val entityPayloads = entityQueryService.retrieve(listOf(beehiveTestCId))
        assertEquals(1, entityPayloads.size)
        assertEquals(beehiveTestCId, entityPayloads[0].entityId)
        assertJsonPayloadsAreEqual(loadSampleData("beehive_expanded.jsonld"), entityPayloads[0].payload.asString())
    }

    @Test
    fun `it should filter existing entities from a list of ids`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        val existingEntities = entityQueryService.filterExistingEntitiesAsIds(listOf(entity01Uri, entity02Uri))
        assertEquals(1, existingEntities.size)
        assertEquals(entity01Uri, existingEntities[0])
    }

    @Test
    fun `it should return an empty list if no ids are provided to the filter on existence`() = runTest {
        val existingEntities = entityQueryService.filterExistingEntitiesAsIds(emptyList())
        assertTrue(existingEntities.isEmpty())
    }

    @Test
    fun `it should check the existence of an entity`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityQueryService.checkEntityExistence(entity01Uri).shouldSucceed()
        entityQueryService.checkEntityExistence(entity02Uri)
            .shouldFail { assert(it is ResourceNotFoundException) }
    }

    @Test
    fun `it should check the state of an entity`() = runTest {
        loadMinimalEntity(entity01Uri, setOf(BEEHIVE_IRI))
            .sampleDataToNgsiLdEntity()
            .map {
                entityService.createEntityPayload(
                    it.second,
                    it.first,
                    now
                )
            }

        entityQueryService.isMarkedAsDeleted(entity01Uri)
            .shouldSucceedWith {
                assertFalse(it)
            }
        entityQueryService.isMarkedAsDeleted(entity02Uri)
            .shouldFail { assert(it is ResourceNotFoundException) }
    }
}
