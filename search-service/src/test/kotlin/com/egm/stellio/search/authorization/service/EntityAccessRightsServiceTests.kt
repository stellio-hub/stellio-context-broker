package com.egm.stellio.search.authorization.service

import arrow.core.Some
import arrow.core.right
import com.egm.stellio.search.authorization.getSubjectInfoForClient
import com.egm.stellio.search.authorization.getSubjectInfoForGroup
import com.egm.stellio.search.authorization.getSubjectInfoForUser
import com.egm.stellio.search.authorization.model.SubjectAccessRight
import com.egm.stellio.search.authorization.model.SubjectReferential
import com.egm.stellio.search.entity.model.EntityPayload
import com.egm.stellio.search.entity.service.EntityPayloadService
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_NAME
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.DATASET_ID_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.ninjasquad.springmockk.SpykBean
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
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

    private val userUuid = "0768A6D5-D87B-4209-9A22-8C40A8961A79"
    private val groupUuid = "220FC854-3609-404B-BC77-F2DFE332B27B"
    private val serviceAccountUuid = "8C55EE65-94EC-407B-9003-33DC37A6A080"
    private val entityId01 = "urn:ngsi-ld:Entity:01".toUri()
    private val entityId02 = "urn:ngsi-ld:Entity:02".toUri()

    @BeforeEach
    fun setDefaultBehaviorOnSubjectReferential() {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } answers { false.right() }
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(userUuid))
        } answers { listOf(userUuid).right() }
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

        entityAccessRightsService.setReadRoleOnEntity(userUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(userUuid), "urn:ngsi-ld:Entity:1111".toUri())
            .shouldSucceed()
    }

    @Test
    fun `it should remove an entity from the allowed list of read entities`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(userUuid, entityId01)
        entityAccessRightsService.removeRoleOnEntity(userUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).fold(
            { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId01"), it) },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should remove an entity from the list of known entities`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(userUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).shouldSucceed()

        entityAccessRightsService.removeRolesOnEntity(entityId01)

        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).fold(
            { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId01"), it) },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should allow the owner of entity to administrate the entity`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setOwnerRoleOnEntity(userUuid, entityId01)

        entityAccessRightsService.canWriteEntity(Some(userUuid), entityId01).shouldSucceed()
        entityAccessRightsService.checkHasRightOnEntity(
            Some(userUuid),
            entityId01,
            emptyList(),
            listOf(AccessRight.IS_OWNER, AccessRight.CAN_ADMIN)
        ).shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should allow an user having a direct read role on a entity`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(userUuid, entityId01)
        entityAccessRightsService.setWriteRoleOnEntity(userUuid, "urn:ngsi-ld:Entity:6666".toUri())

        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(userUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
            {
                assertEquals(
                    AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                    it
                )
            },
            { fail("it should have not read right on entity") }
        )
        entityAccessRightsService.canWriteEntity(Some(userUuid), "urn:ngsi-ld:Entity:6666".toUri()).shouldSucceed()
    }

    @Test
    fun `it should allow an user having a read role on a entity via a group membership`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(userUuid))
        } returns listOf(groupUuid, userUuid).right()

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(userUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
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
            subjectReferentialService.getSubjectAndGroupsUUID(Some(userUuid))
        } answers { listOf(groupUuid, userUuid).right() }
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns false.right()
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId01)
        entityAccessRightsService.setReadRoleOnEntity(userUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(userUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
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
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns true.right()

        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(userUuid), "urn:ngsi-ld:Entity:2222".toUri()).shouldSucceed()

        coVerify {
            subjectReferentialService.hasStellioAdminRole(listOf(userUuid))
            subjectReferentialService.retrieve(eq(userUuid)) wasNot Called
        }
    }

    @Test
    fun `it should delete entity access rights associated to an user`() = runTest {
        entityAccessRightsService.setReadRoleOnEntity(userUuid, entityId01)

        entityAccessRightsService.delete(userUuid).shouldSucceed()
    }

    @Test
    fun `it should allow user who have read right on entity`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(userUuid, entityId01)

        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).shouldSucceed()
    }

    @Test
    fun `it should allow user who have write right on entity`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setWriteRoleOnEntity(userUuid, entityId01)

        entityAccessRightsService.canWriteEntity(Some(userUuid), entityId01).shouldSucceed()
    }

    @Test
    fun `it should not allow user who have not read right on entity and entity has not a specific access policy`() =
        runTest {
            coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

            runBlocking {
                entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).fold(
                    { assertEquals("User forbidden read access to entity $entityId01", it.message) },
                    { fail("it should have not read right on entity") }
                )
            }
        }

    @Test
    fun `it should not allow user who have not write right on entity and entity has not a specific access policy`() =
        runTest {
            coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

            entityAccessRightsService.canWriteEntity(Some(userUuid), entityId01).fold(
                { assertEquals("User forbidden write access to entity $entityId01", it.message) },
                { fail("it should have not write right on entity") }
            )
        }

    @Test
    fun `it should allow user it has no right on entity but entity has a specific access policy`() = runTest {
        coEvery { entityPayloadService.hasSpecificAccessPolicies(any(), any()) } returns true.right()

        entityAccessRightsService.canWriteEntity(Some(userUuid), entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(userUuid), entityId01).shouldSucceed()

        coVerify { subjectReferentialService.getSubjectAndGroupsUUID(Some(userUuid)) wasNot Called }
    }

    @Test
    fun `it should find if user has ownership on an entity`() = runTest {
        entityAccessRightsService.setOwnerRoleOnEntity(userUuid, entityId01).shouldSucceed()

        entityAccessRightsService.isOwnerOfEntity(userUuid, entityId01).shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should get all the entities an user has created with appropriate other rights`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE))
        entityAccessRightsService.setOwnerRoleOnEntity(userUuid, entityId01).shouldSucceed()
        entityAccessRightsService.setWriteRoleOnEntity(serviceAccountUuid, entityId01).shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            emptyList(),
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(BEEHIVE_TYPE, entityAccessControl.types[0])
            assertEquals(AccessRight.IS_OWNER, entityAccessControl.right)
            assertNull(entityAccessControl.specificAccessPolicy)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(userUuid),
            emptyList()
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            emptyList(),
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(BEEHIVE_TYPE, entityAccessControl.types[0])
            assertEquals(AccessRight.CAN_WRITE, entityAccessControl.right)
            assertEquals(AUTH_READ, entityAccessControl.specificAccessPolicy)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(userUuid),
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
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            emptyList(),
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(2, it.size)
            it.forEach { entityAccessControl ->
                assertEquals(AccessRight.CAN_ADMIN, entityAccessControl.right)
            }
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(userUuid),
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
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId03, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            emptyList(),
            BEEHIVE_TYPE,
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(BEEHIVE_TYPE, entityAccessControl.types[0])
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(userUuid),
            emptyList(),
            BEEHIVE_TYPE
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt ids`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        createEntityPayload(entityId03, setOf(APIARY_TYPE))
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId03, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            emptyList(),
            null,
            setOf(entityId01, entityId02),
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            assertEquals(entityId01, it[0].id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(userUuid),
            emptyList(),
            BEEHIVE_TYPE,
            setOf(entityId01, entityId03)
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt ids and types`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        createEntityPayload(entityId03, setOf(APIARY_TYPE))
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId03, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            emptyList(),
            BEEHIVE_TYPE,
            setOf(entityId01, entityId03),
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            assertEquals(entityId01, it[0].id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(userUuid),
            emptyList(),
            BEEHIVE_TYPE,
            setOf(entityId01, entityId03)
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
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId03, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            listOf(AccessRight.CAN_WRITE),
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId03, entityAccessControl.id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(userUuid),
            listOf(AccessRight.CAN_WRITE)
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt access rights and types`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        createEntityPayload(entityId03, setOf(APIARY_TYPE))
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId02, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId03, AccessRight.CAN_READ).shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            listOf(AccessRight.CAN_WRITE),
            "$BEEHIVE_TYPE,$APIARY_TYPE",
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId02, entityAccessControl.id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            Some(userUuid),
            listOf(AccessRight.CAN_WRITE)
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should return only one entity with higher right if user has access through different paths`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE))
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(userUuid))
        } returns listOf(groupUuid, userUuid).right()

        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE)
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId01, AccessRight.CAN_ADMIN)

        entityAccessRightsService.getSubjectAccessRights(
            Some(userUuid),
            emptyList(),
            BEEHIVE_TYPE,
            paginationQuery = PaginationQuery(limit = 100, offset = 0)
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(AccessRight.CAN_ADMIN, entityAccessControl.right)
        }
    }

    @Test
    fun `it should return nothing when list of entities is empty`() = runTest {
        entityAccessRightsService.getAccessRightsForEntities(Some(userUuid), emptyList())
            .shouldSucceedWith { assertTrue(it.isEmpty()) }
    }

    @Test
    fun `it should get other subject rights for one entity and a group`() = runTest {
        createSubjectReferential(userUuid, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(groupUuid, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))

        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId01, AccessRight.CAN_READ).shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(Some(userUuid), listOf(entityId01, entityId02))
            .shouldSucceedWith {
                assertEquals(1, it.size)
                val result = it.entries.first()
                assertEquals(entityId01, result.key)
                assertEquals(1, result.value.size)
                assertTrue(result.value.containsKey(AccessRight.CAN_READ))
                val rCanReadList = result.value[AccessRight.CAN_READ]!!
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
        createSubjectReferential(userUuid, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(groupUuid, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))

        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId01, AccessRight.CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(userUuid, entityId02, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId02, AccessRight.CAN_WRITE).shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(Some(userUuid), listOf(entityId01, entityId02))
            .shouldSucceedWith {
                assertEquals(2, it.size)
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.CAN_READ))
                assertTrue(it.getValue(entityId02).containsKey(AccessRight.CAN_WRITE))
            }
    }

    @Test
    fun `it should get other subject rights for all kinds of subjects`() = runTest {
        createSubjectReferential(userUuid, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(groupUuid, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))
        createSubjectReferential(serviceAccountUuid, SubjectType.CLIENT, getSubjectInfoForClient("client-id", "kc-id"))

        entityAccessRightsService.setRoleOnEntity(userUuid, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(groupUuid, entityId01, AccessRight.CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(serviceAccountUuid, entityId01, AccessRight.CAN_ADMIN).shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(Some(userUuid), listOf(entityId01))
            .shouldSucceedWith {
                assertEquals(1, it.size)
                val result = it.entries.first()
                assertEquals(2, result.value.size)
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.CAN_READ))
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.CAN_ADMIN))
                val rCanAdminList = it.getValue(entityId01).getValue(AccessRight.CAN_ADMIN)
                assertEquals(1, rCanAdminList.size)
                assertEquals(CLIENT_ENTITY_PREFIX + serviceAccountUuid, rCanAdminList[0].uri.toString())
            }
    }

    @Test
    fun `it should get Ids of all entities owned by a user`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE))
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        entityAccessRightsService.setOwnerRoleOnEntity(userUuid, entityId01).shouldSucceed()
        entityAccessRightsService.setWriteRoleOnEntity(userUuid, entityId02).shouldSucceed()
        entityAccessRightsService.getEntitiesIdsOwnedBySubject(userUuid)
            .shouldSucceedWith {
                assertEquals(1, it.size)
                assertEquals(entityId01, it[0])
            }
    }

    @Test
    fun `it should delete all access rights on entities`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_TYPE))
        createEntityPayload(entityId02, setOf(BEEHIVE_TYPE))
        entityAccessRightsService.setOwnerRoleOnEntity(userUuid, entityId01).shouldSucceed()
        entityAccessRightsService.setWriteRoleOnEntity(userUuid, entityId02).shouldSucceed()
        val entitiesIds = listOf(entityId01, entityId02)
        entityAccessRightsService.deleteAllAccessRightsOnEntities(entitiesIds)
            .shouldSucceed()
        entityAccessRightsService.getAccessRightsForEntities(Some(userUuid), entitiesIds)
            .shouldSucceedWith { assertTrue(it.isEmpty()) }
    }
    private suspend fun createEntityPayload(
        entityId: URI,
        types: Set<ExpandedTerm>,
        specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null
    ) {
        val rawEntity =
            if (specificAccessPolicy != null)
                loadMinimalEntityWithSap(entityId, types, specificAccessPolicy, AUTHZ_TEST_COMPOUND_CONTEXTS)
            else loadMinimalEntity(entityId, types)
        rawEntity.sampleDataToNgsiLdEntity().map {
            entityPayloadService.createEntityPayload(
                ngsiLdEntity = it.second,
                expandedEntity = it.first,
                createdAt = ngsiLdDateTime()
            )
        }
    }

    private suspend fun createSubjectReferential(
        subjectId: String,
        subjectType: SubjectType,
        subjectInfo: Json
    ) {
        subjectReferentialService.create(
            SubjectReferential(
                subjectId = subjectId,
                subjectType = subjectType,
                subjectInfo = subjectInfo
            )
        )
    }
}
