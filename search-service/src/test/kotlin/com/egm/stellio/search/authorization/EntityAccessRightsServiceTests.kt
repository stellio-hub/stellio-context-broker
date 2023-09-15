package com.egm.stellio.search.authorization

import arrow.core.Some
import arrow.core.right
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_NAME
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.JsonLdUtils.DATASET_ID_PREFIX
import com.ninjasquad.springmockk.SpykBean
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class EntityAccessRightsServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @SpykBean
    private lateinit var subjectReferentialService: SubjectReferentialService

    @SpykBean
    private lateinit var entityPayloadService: EntityPayloadService

    private val subjectUuid = "0768A6D5-D87B-4209-9A22-8C40A8961A79"
    private val groupUuid = "220FC854-3609-404B-BC77-F2DFE332B27B"
    private val clientUuid = "8C55EE65-94EC-407B-9003-33DC37A6A080"
    private val entityId01 = "urn:ngsi-ld:Entity:01".toUri()
    private val entityId02 = "urn:ngsi-ld:Entity:02".toUri()

    @BeforeEach
    fun setDefaultBehaviorOnSubjectReferential() {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid)) } answers { false.right() }
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } answers { listOf(subjectUuid).right() }
    }

    @AfterEach
    fun clearEntityAccessRightsTable() {
        r2dbcEntityTemplate.delete<SubjectAccessRight>().from("entity_access_rights").all().block()
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should add a new entity in the allowed list of read entities`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns true.right()

        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:1111".toUri())
            .shouldSucceed()
    }

    @Test
    fun `it should remove an entity from the allowed list of read entities`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId01)
        entityAccessRightsService.removeRoleOnEntity(subjectUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).fold(
            { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId01"), it) },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should remove an entity from the list of known entities`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setAdminRoleOnEntity(subjectUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).shouldSucceed()

        entityAccessRightsService.removeRolesOnEntity(entityId01)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).fold(
            { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId01"), it) },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should allow an user having a direct read role on a entity`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId01)
        entityAccessRightsService.setWriteRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:6666".toUri())

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
            {
                assertEquals(
                    AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                    it
                )
            },
            { fail("it should have not read right on entity") }
        )
        entityAccessRightsService.canWriteEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:6666".toUri()).shouldSucceed()
    }

    @Test
    fun `it should allow an user having a read role on a entity via a group membership`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } returns listOf(groupUuid, subjectUuid).right()

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
            {
                assertEquals(
                    AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                    it
                )
            },
            { fail("it should not have read right on entity") }
        )
    }

    @Test
    fun `it should allow an user having a read role on a entity both directly and via a group membership`() = runTest {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } answers { listOf(groupUuid, subjectUuid).right() }
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns false.right()
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId01)
        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
            {
                assertEquals(
                    AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                    it
                )
            },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should allow an user having the stellio-admin role to read any entity`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid)) } returns true.right()

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).shouldSucceed()

        coVerify {
            subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid))
            subjectReferentialService.retrieve(eq(subjectUuid)) wasNot Called
        }
    }

    @Test
    fun `it should delete entity access rights associated to an user`() = runTest {
        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId01)

        entityAccessRightsService.delete(subjectUuid).shouldSucceed()
    }

    @Test
    fun `it should allow user who have read right on entity`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).shouldSucceed()
    }

    @Test
    fun `it should allow user who have write right on entity`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setWriteRoleOnEntity(subjectUuid, entityId01)

        entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId01).shouldSucceed()
    }

    @Test
    fun `it should not allow user who have not read right on entity and entity has not a specific access policy`() =
        runTest {
            coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

            runBlocking {
                entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).fold(
                    { assertEquals("User forbidden read access to entity $entityId01", it.message) },
                    { fail("it should have not read right on entity") }
                )
            }
        }

    @Test
    fun `it should not allow user who have not write right on entity and entity has not a specific access policy`() =
        runTest {
            coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

            entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId01).fold(
                { assertEquals("User forbidden write access to entity $entityId01", it.message) },
                { fail("it should have not write right on entity") }
            )
        }

    @Test
    fun `it should allow user it has no right on entity but entity has a specific access policy`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns true.right()

        entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId01).shouldSucceed()

        coVerify { subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid)) wasNot Called }
    }

    @Test
    fun `it should get all entities an user has access to`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId01, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.R_CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(subjectUuid),
            emptyList(),
            limit = 100,
            offset = 0
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(BEEHIVE_TYPE, entityAccessControl.types[0])
            assertEquals(AccessRight.R_CAN_WRITE, entityAccessControl.right)
            assertEquals(AUTH_READ, entityAccessControl.specificAccessPolicy)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(subjectUuid),
            emptyList()
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities for a stellio-admin user`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns true.right()

        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId01, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.R_CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(subjectUuid),
            emptyList(),
            limit = 100,
            offset = 0
        ).shouldSucceedWith {
            assertEquals(2, it.size)
            it.forEach { entityAccessControl ->
                assertEquals(AccessRight.R_CAN_ADMIN, entityAccessControl.right)
            }
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(subjectUuid),
            emptyList()
        ).shouldSucceedWith {
            assertEquals(2, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt types`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        createEntityPayload(entityId03, setOf(APIARY_TYPE))
        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId01, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId03, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.R_CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(subjectUuid),
            emptyList(),
            BEEHIVE_TYPE,
            100,
            0
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(BEEHIVE_TYPE, entityAccessControl.types[0])
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(subjectUuid),
            emptyList(),
            BEEHIVE_TYPE
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt access rights`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        createEntityPayload(entityId03, setOf(APIARY_TYPE))
        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId01, AccessRight.R_CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId03, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.R_CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(subjectUuid),
            listOf(AccessRight.R_CAN_WRITE),
            limit = 100,
            offset = 0
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId03, entityAccessControl.id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(subjectUuid),
            listOf(AccessRight.R_CAN_WRITE)
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should return only one entity with higher right if user has access through different paths`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE))
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } returns listOf(groupUuid, subjectUuid).right()

        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId01, AccessRight.R_CAN_WRITE)
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId01, AccessRight.R_CAN_ADMIN)

        entityAccessRightsService.getSubjectAccessRights(
            Some(subjectUuid),
            emptyList(),
            BEEHIVE_TYPE,
            100,
            0
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(AccessRight.R_CAN_ADMIN, entityAccessControl.right)
        }
    }

    @Test
    fun `it should return nothing when list of entities is empty`() = runTest {
        entityAccessRightsService.getAccessRightsForEntities(Some(subjectUuid), emptyList())
            .shouldSucceedWith { assertTrue(it.isEmpty()) }
    }

    @Test
    fun `it should get other subject rights for one entity and a group`() = runTest {
        createSubjectReferential(subjectUuid, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(groupUuid, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))

        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId01, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId01, AccessRight.R_CAN_READ).shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(Some(subjectUuid), listOf(entityId01, entityId02))
            .shouldSucceedWith {
                assertEquals(1, it.size)
                val result = it.entries.first()
                assertEquals(entityId01, result.key)
                assertEquals(1, result.value.size)
                assertTrue(result.value.containsKey(AccessRight.R_CAN_READ))
                val rCanReadList = result.value[AccessRight.R_CAN_READ]!!
                assertEquals(1, rCanReadList.size)
                val subjectRightDetail = rCanReadList[0]
                assertEquals(GROUP_ENTITY_PREFIX + groupUuid, subjectRightDetail.uri.toString())
                assertEquals(DATASET_ID_PREFIX + groupUuid, subjectRightDetail.datasetId.toString())
                assertEquals(2, subjectRightDetail.subjectInfo.size)
                assertTrue(subjectRightDetail.subjectInfo.containsKey(AUTH_TERM_NAME))
                assertEquals("Stellio Team", subjectRightDetail.subjectInfo[AUTH_TERM_NAME])
                assertTrue(subjectRightDetail.subjectInfo.containsKey("kind"))
            }
    }

    @Test
    fun `it should get other subject rights for a set of entities`() = runTest {
        createSubjectReferential(subjectUuid, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(groupUuid, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))

        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId01, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId01, AccessRight.R_CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId02, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId02, AccessRight.R_CAN_WRITE).shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(Some(subjectUuid), listOf(entityId01, entityId02))
            .shouldSucceedWith {
                assertEquals(2, it.size)
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.R_CAN_READ))
                assertTrue(it.getValue(entityId02).containsKey(AccessRight.R_CAN_WRITE))
            }
    }

    @Test
    fun `it should get other subject rights for all kinds of subjects`() = runTest {
        createSubjectReferential(subjectUuid, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(groupUuid, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))
        createSubjectReferential(
            UUID.randomUUID().toString(),
            SubjectType.CLIENT,
            getSubjectInfoForClient("IoT Device"),
            clientUuid
        )

        entityAccessRightsService.setRoleOnEntity(subjectUuid, entityId01, AccessRight.R_CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId01, AccessRight.R_CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(clientUuid, entityId01, AccessRight.R_CAN_ADMIN).shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(Some(subjectUuid), listOf(entityId01))
            .shouldSucceedWith {
                assertEquals(1, it.size)
                val result = it.entries.first()
                assertEquals(2, result.value.size)
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.R_CAN_READ))
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.R_CAN_ADMIN))
                val rCanAdminList = it.getValue(entityId01).getValue(AccessRight.R_CAN_ADMIN)
                assertEquals(1, rCanAdminList.size)
                assertEquals(CLIENT_ENTITY_PREFIX + clientUuid, rCanAdminList[0].uri.toString())
            }
    }

    private suspend fun createEntityPayload(
        entityId: URI,
        types: Set<ExpandedTerm>,
        specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null
    ) {
        val rawEntity =
            if (specificAccessPolicy != null)
                loadMinimalEntityWithSap(entityId, types, specificAccessPolicy, setOf(AUTHORIZATION_COMPOUND_CONTEXT))
            else loadMinimalEntity(entityId, types)
        rawEntity.sampleDataToNgsiLdEntity().map {
            entityPayloadService.createEntityPayload(
                ngsiLdEntity = it.second,
                jsonLdEntity = it.first,
                createdAt = ngsiLdDateTime()
            )
        }
    }

    private suspend fun createSubjectReferential(
        subjectId: String,
        subjectType: SubjectType,
        subjectInfo: Json,
        serviceAccountId: String? = null
    ) {
        subjectReferentialService.create(
            SubjectReferential(
                subjectId = subjectId,
                subjectType = subjectType,
                subjectInfo = subjectInfo,
                serviceAccountId = serviceAccountId
            )
        )
    }
}
