package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
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

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class EntityPayloadServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityPayloadService: EntityPayloadService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val entityUri = "urn:ngsi-ld:Entity:01".toUri()
    private val now = Instant.now().atZone(ZoneOffset.UTC)

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should create an entity payload if none existed yet`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        ).shouldSucceed()
    }

    @Test
    fun `it should create an entity payload with specificAccessPolicy`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            SpecificAccessPolicy.AUTH_READ
        )
        entityPayloadService.createEntityPayload(
            "urn:ngsi-ld:Entity:02".toUri(),
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            SpecificAccessPolicy.AUTH_WRITE
        )
        entityPayloadService.hasSpecificAccessPolicies(
            entityUri,
            listOf(SpecificAccessPolicy.AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            entityUri,
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }

        entityPayloadService.hasSpecificAccessPolicies(
            "urn:ngsi-ld:Entity:02".toUri(),
            listOf(SpecificAccessPolicy.AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            "urn:ngsi-ld:Entity:02".toUri(),
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should not create an entity payload if one already existed`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        )

        assertThrows<DataIntegrityViolationException> {
            entityPayloadService.createEntityPayload(
                entityUri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
            )
        }
    }

    @Test
    fun `it should retrieve an entity payload`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        ).shouldSucceed()

        entityPayloadService.retrieve(entityUri)
            .shouldSucceedWith {
                assertThat(it)
                    .hasFieldOrPropertyWithValue("entityId", entityUri)
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
            entityUri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            SpecificAccessPolicy.AUTH_READ
        )

        entityPayloadService.retrieve(entityUri)
            .shouldSucceedWith {
                assertThat(it)
                    .hasFieldOrPropertyWithValue("entityId", entityUri)
                    .hasFieldOrPropertyWithValue("types", listOf(BEEHIVE_TYPE))
                    .hasFieldOrPropertyWithValue("createdAt", now)
                    .hasFieldOrPropertyWithValue("modifiedAt", null)
                    .hasFieldOrPropertyWithValue("contexts", listOf(NGSILD_CORE_CONTEXT))
                    .hasFieldOrPropertyWithValue("specificAccessPolicy", SpecificAccessPolicy.AUTH_READ)
            }
    }

    @Test
    fun `it should upsert an entity payload if one already existed`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        )

        entityPayloadService.upsertEntityPayload(entityUri, EMPTY_PAYLOAD)
            .shouldSucceed()
    }

    @Test
    fun `it should update the types of a temporal entity`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        )

        entityPayloadService.updateTypes(entityUri, listOf(BEEHIVE_TYPE, APIARY_TYPE))
            .shouldSucceedWith {
                it.isSuccessful()
            }

        entityPayloadService.retrieve(entityUri)
            .shouldSucceedWith {
                assertEquals(listOf(BEEHIVE_TYPE, APIARY_TYPE), it.types)
            }
    }

    @Test
    fun `it should update a specific access policy for a temporal entity`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            SpecificAccessPolicy.AUTH_READ
        )
        entityPayloadService.createEntityPayload(
            "urn:ngsi-ld:Entity:02".toUri(),
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            SpecificAccessPolicy.AUTH_READ
        )

        entityPayloadService.updateSpecificAccessPolicy(
            entityUri,
            SpecificAccessPolicy.AUTH_WRITE
        ).shouldSucceed()

        entityPayloadService.hasSpecificAccessPolicies(
            entityUri,
            listOf(SpecificAccessPolicy.AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            entityUri,
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertTrue(it) }
        entityPayloadService.hasSpecificAccessPolicies(
            "urn:ngsi-ld:Entity:02".toUri(),
            listOf(SpecificAccessPolicy.AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should delete an entity payload`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        )

        val deleteResult = entityPayloadService.deleteEntityPayload(entityUri)
        assertTrue(deleteResult.isRight())

        // if correctly deleted, we should be able to create a new one
        entityPayloadService.createEntityPayload(
            entityUri, listOf(BEEHIVE_TYPE), now, EMPTY_PAYLOAD, listOf(NGSILD_CORE_CONTEXT)
        ).shouldSucceed()
    }

    @Test
    fun `it should remove a specific access policy from a entity payload`() = runTest {
        entityPayloadService.createEntityPayload(
            entityUri,
            listOf(BEEHIVE_TYPE),
            now,
            EMPTY_PAYLOAD,
            listOf(NGSILD_CORE_CONTEXT),
            AuthContextModel.SpecificAccessPolicy.AUTH_READ
        )

        entityPayloadService.hasSpecificAccessPolicies(
            entityUri,
            listOf(AuthContextModel.SpecificAccessPolicy.AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityPayloadService.removeSpecificAccessPolicy(entityUri).shouldSucceed()
        entityPayloadService.hasSpecificAccessPolicies(
            entityUri,
            listOf(AuthContextModel.SpecificAccessPolicy.AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
    }
}
