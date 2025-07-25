package com.egm.stellio.search.authorization.service

import arrow.core.right
import com.egm.stellio.search.authorization.GROUP_UUID
import com.egm.stellio.search.authorization.SERVICE_ACCOUNT_UUID
import com.egm.stellio.search.authorization.USER_UUID
import com.egm.stellio.search.authorization.getSubjectInfoForClient
import com.egm.stellio.search.authorization.getSubjectInfoForGroup
import com.egm.stellio.search.authorization.getSubjectInfoForUser
import com.egm.stellio.search.authorization.model.SubjectAccessRight
import com.egm.stellio.search.authorization.model.SubjectReferential
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.buildSapAttribute
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.AUTHZ_TEST_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_NAME
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.DATASET_ID_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_WRITE
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.loadMinimalEntity
import com.egm.stellio.shared.util.loadMinimalEntityWithSap
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.sampleDataToNgsiLdEntity
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.SpykBean
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
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

@SpringBootTest
@ActiveProfiles("test")
class EntityAccessRightsServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @SpykBean
    private lateinit var subjectReferentialService: SubjectReferentialService

    @SpykBean
    private lateinit var entityService: EntityService

    private val entityId01 = "urn:ngsi-ld:Entity:01".toUri()
    private val entityId02 = "urn:ngsi-ld:Entity:02".toUri()

    @BeforeEach
    fun setDefaultBehaviorOnSubjectReferential() {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(USER_UUID)) } answers { false.right() }
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } answers { listOf(USER_UUID).right() }
    }

    @AfterEach
    fun clearEntityAccessRightsTable() {
        r2dbcEntityTemplate.delete<SubjectAccessRight>().from("entity_access_rights").all().block()
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
        clearAllMocks()
    }

    @Test
    fun `it should add a new entity in the allowed list of read entities`() = runTest {
        entityAccessRightsService.setReadRoleOnEntity(USER_UUID, entityId01)

        entityAccessRightsService.canReadEntity(entityId01)
            .shouldSucceed()
    }

    @Test
    fun `it should remove an entity from the allowed list of read entities`() = runTest {
        entityAccessRightsService.setReadRoleOnEntity(USER_UUID, entityId01)
        entityAccessRightsService.removeRoleOnEntity(USER_UUID, entityId01)

        entityAccessRightsService.canReadEntity(entityId01).fold(
            { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId01"), it) },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should remove an entity from the list of known entities`() = runTest {
        entityAccessRightsService.setReadRoleOnEntity(USER_UUID, entityId01)

        entityAccessRightsService.canReadEntity(entityId01).shouldSucceed()

        entityAccessRightsService.removeRolesOnEntity(entityId01)

        entityAccessRightsService.canReadEntity(entityId01).fold(
            { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId01"), it) },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should allow the owner of entity to administrate the entity`() = runTest {
        entityAccessRightsService.setOwnerRoleOnEntity(USER_UUID, entityId01)

        entityAccessRightsService.canWriteEntity(entityId01).shouldSucceed()
        entityAccessRightsService.checkHasRightOnEntity(
            entityId01,
            emptyList(),
            listOf(AccessRight.IS_OWNER, AccessRight.CAN_ADMIN)
        ).shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should allow an user having a direct read role on a entity`() = runTest {
        entityAccessRightsService.setReadRoleOnEntity(USER_UUID, entityId01)
        entityAccessRightsService.setWriteRoleOnEntity(USER_UUID, "urn:ngsi-ld:Entity:6666".toUri())

        entityAccessRightsService.canReadEntity(entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity("urn:ngsi-ld:Entity:2222".toUri()).fold(
            {
                assertEquals(
                    AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                    it
                )
            },
            { fail("it should have not read right on entity") }
        )
        entityAccessRightsService.canWriteEntity("urn:ngsi-ld:Entity:6666".toUri()).shouldSucceed()
    }

    @Test
    fun `it should allow an user having a read role on a entity via a group membership`() = runTest {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(USER_UUID, GROUP_UUID).right()

        entityAccessRightsService.setReadRoleOnEntity(GROUP_UUID, entityId01)

        entityAccessRightsService.canReadEntity(entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity("urn:ngsi-ld:Entity:2222".toUri()).fold(
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
            subjectReferentialService.getSubjectAndGroupsUUID()
        } answers { listOf(USER_UUID, GROUP_UUID).right() }
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(GROUP_UUID, entityId01)
        entityAccessRightsService.setReadRoleOnEntity(USER_UUID, entityId01)

        entityAccessRightsService.canReadEntity(entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity("urn:ngsi-ld:Entity:2222".toUri()).fold(
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
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(USER_UUID)) } returns true.right()

        entityAccessRightsService.canReadEntity(entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity("urn:ngsi-ld:Entity:2222".toUri()).shouldSucceed()

        coVerify {
            subjectReferentialService.hasStellioAdminRole(listOf(USER_UUID))
        }
        coVerify(exactly = 0) {
            subjectReferentialService.retrieve(eq(USER_UUID))
        }
    }

    @Test
    fun `it should delete entity access rights associated to an user`() = runTest {
        entityAccessRightsService.setReadRoleOnEntity(USER_UUID, entityId01)

        entityAccessRightsService.delete(USER_UUID).shouldSucceed()

        entityAccessRightsService.canReadEntity(entityId01).shouldFailWith {
            it is AccessDeniedException
        }
    }

    @Test
    fun `it should allow user who have write right on entity`() = runTest {
        entityAccessRightsService.setWriteRoleOnEntity(USER_UUID, entityId01)

        entityAccessRightsService.canWriteEntity(entityId01).shouldSucceed()
    }

    @Test
    fun `it should find if user has ownership on an entity`() = runTest {
        entityAccessRightsService.setOwnerRoleOnEntity(USER_UUID, entityId01).shouldSucceed()

        entityAccessRightsService.isOwnerOfEntity(USER_UUID, entityId01).shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should get all the entities an user has created with appropriate other rights`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI))
        entityAccessRightsService.setOwnerRoleOnEntity(USER_UUID, entityId01).shouldSucceed()
        entityAccessRightsService.setWriteRoleOnEntity(SERVICE_ACCOUNT_UUID, entityId01).shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(BEEHIVE_IRI, entityAccessControl.types[0])
            assertEquals(AccessRight.IS_OWNER, entityAccessControl.right)
            assertNull(entityAccessControl.specificAccessPolicy)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            emptyList()
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(BEEHIVE_IRI, entityAccessControl.types[0])
            assertEquals(AccessRight.CAN_WRITE, entityAccessControl.right)
            assertEquals(AUTH_READ, entityAccessControl.specificAccessPolicy)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            emptyList()
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities for a stellio-admin user`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns true.right()

        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(2, it.size)
            it.forEach { entityAccessControl ->
                assertEquals(AccessRight.CAN_ADMIN, entityAccessControl.right)
            }
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            emptyList()
        ).shouldSucceedWith {
            assertEquals(2, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt types`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        createEntityPayload(entityId03, setOf(APIARY_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId03, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                typeSelection = BEEHIVE_IRI,
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(BEEHIVE_IRI, entityAccessControl.types[0])
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            emptyList(),
            BEEHIVE_IRI
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt ids`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        createEntityPayload(entityId03, setOf(APIARY_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId03, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                ids = setOf(entityId01, entityId02),
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            assertEquals(entityId01, it[0].id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            emptyList(),
            BEEHIVE_IRI,
            setOf(entityId01, entityId03)
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt ids and types`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        createEntityPayload(entityId03, setOf(APIARY_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId03, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                ids = setOf(entityId01, entityId02),
                typeSelection = BEEHIVE_IRI,
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            assertEquals(entityId01, it[0].id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            emptyList(),
            BEEHIVE_IRI,
            setOf(entityId01, entityId03)
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt access rights`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        createEntityPayload(entityId03, setOf(APIARY_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId03, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(UUID.randomUUID().toString(), entityId02, AccessRight.CAN_WRITE)
            .shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            listOf(AccessRight.CAN_WRITE),
            entitiesQuery = EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId03, entityAccessControl.id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            listOf(AccessRight.CAN_WRITE)
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should get all entities an user has access to wrt access rights and types`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()

        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        createEntityPayload(entityId03, setOf(APIARY_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId02, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId03, AccessRight.CAN_READ).shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            listOf(AccessRight.CAN_WRITE),
            entitiesQuery = EntitiesQueryFromGet(
                typeSelection = "$BEEHIVE_IRI,$APIARY_IRI",
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId02, entityAccessControl.id)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            listOf(AccessRight.CAN_WRITE)
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should return only one entity with higher right if user has access through different paths`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI))
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(USER_UUID, GROUP_UUID).right()

        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE)
        entityAccessRightsService.setRoleOnEntity(GROUP_UUID, entityId01, AccessRight.CAN_ADMIN)

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                typeSelection = BEEHIVE_IRI,
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith {
            assertEquals(1, it.size)
            val entityAccessControl = it[0]
            assertEquals(entityId01, entityAccessControl.id)
            assertEquals(AccessRight.CAN_ADMIN, entityAccessControl.right)
        }
    }

    @Test
    fun `getSubjectAccessRights should include deleted entities if it is asked for`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()

        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId02, AccessRight.CAN_ADMIN).shouldSucceed()
        entityService.deleteEntity(entityId02).shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            ),
            includeDeleted = true
        ).shouldSucceedWith { entityAccessRights ->
            assertEquals(2, entityAccessRights.size)
            assertEquals(1, entityAccessRights.filter { it.isDeleted }.size)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            emptyList(),
            includeDeleted = true
        ).shouldSucceedWith {
            assertEquals(2, it)
        }
    }

    @Test
    fun `getSubjectAccessRights should not include deleted entities if it is not asked for`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()

        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId02, AccessRight.CAN_ADMIN).shouldSucceed()
        entityService.deleteEntity(entityId02).shouldSucceed()

        entityAccessRightsService.getSubjectAccessRights(
            emptyList(),
            entitiesQuery = EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 100, offset = 0),
                contexts = emptyList()
            )
        ).shouldSucceedWith { entityAccessRights ->
            assertEquals(1, entityAccessRights.size)
            assertEquals(0, entityAccessRights.filter { it.isDeleted }.size)
        }

        entityAccessRightsService.getSubjectAccessRightsCount(
            emptyList()
        ).shouldSucceedWith {
            assertEquals(1, it)
        }
    }

    @Test
    fun `it should return nothing when list of entities is empty`() = runTest {
        entityAccessRightsService.getAccessRightsForEntities(emptyList())
            .shouldSucceedWith { assertTrue(it.isEmpty()) }
    }

    @Test
    fun `it should get other subject rights for one entity and a group`() = runTest {
        createSubjectReferential(USER_UUID, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(GROUP_UUID, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))

        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(GROUP_UUID, entityId01, AccessRight.CAN_READ).shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(listOf(entityId01, entityId02))
            .shouldSucceedWith {
                assertEquals(1, it.size)
                val result = it.entries.first()
                assertEquals(entityId01, result.key)
                assertEquals(1, result.value.size)
                assertTrue(result.value.containsKey(AccessRight.CAN_READ))
                val rCanReadList = result.value[AccessRight.CAN_READ]!!
                assertEquals(1, rCanReadList.size)
                val subjectRightDetail = rCanReadList[0]
                assertEquals(GROUP_ENTITY_PREFIX + GROUP_UUID, subjectRightDetail.uri.toString())
                assertEquals(DATASET_ID_PREFIX + GROUP_UUID, subjectRightDetail.datasetId.toString())
                assertEquals(2, subjectRightDetail.subjectInfo.size)
                assertTrue(subjectRightDetail.subjectInfo.containsKey(AUTH_TERM_NAME))
                assertEquals("Stellio Team", subjectRightDetail.subjectInfo[AUTH_TERM_NAME])
                assertTrue(subjectRightDetail.subjectInfo.containsKey("kind"))
            }
    }

    @Test
    fun `it should get other subject rights for a set of entities`() = runTest {
        createSubjectReferential(USER_UUID, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(GROUP_UUID, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))

        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(GROUP_UUID, entityId01, AccessRight.CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId02, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(GROUP_UUID, entityId02, AccessRight.CAN_WRITE).shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(listOf(entityId01, entityId02))
            .shouldSucceedWith {
                assertEquals(2, it.size)
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.CAN_READ))
                assertTrue(it.getValue(entityId02).containsKey(AccessRight.CAN_WRITE))
            }
    }

    @Test
    fun `it should get other subject rights for all kinds of subjects`() = runTest {
        createSubjectReferential(USER_UUID, SubjectType.USER, getSubjectInfoForUser("stellio"))
        createSubjectReferential(GROUP_UUID, SubjectType.GROUP, getSubjectInfoForGroup("Stellio Team"))
        createSubjectReferential(
            SERVICE_ACCOUNT_UUID,
            SubjectType.CLIENT,
            getSubjectInfoForClient("client-id", "kc-id")
        )

        entityAccessRightsService.setRoleOnEntity(USER_UUID, entityId01, AccessRight.CAN_WRITE).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(GROUP_UUID, entityId01, AccessRight.CAN_READ).shouldSucceed()
        entityAccessRightsService.setRoleOnEntity(SERVICE_ACCOUNT_UUID, entityId01, AccessRight.CAN_ADMIN)
            .shouldSucceed()

        entityAccessRightsService.getAccessRightsForEntities(listOf(entityId01))
            .shouldSucceedWith {
                assertEquals(1, it.size)
                val result = it.entries.first()
                assertEquals(2, result.value.size)
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.CAN_READ))
                assertTrue(it.getValue(entityId01).containsKey(AccessRight.CAN_ADMIN))
                val rCanAdminList = it.getValue(entityId01).getValue(AccessRight.CAN_ADMIN)
                assertEquals(1, rCanAdminList.size)
                assertEquals(CLIENT_ENTITY_PREFIX + SERVICE_ACCOUNT_UUID, rCanAdminList[0].uri.toString())
            }
    }

    @Test
    fun `it should get ids of all entities owned by a user`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI))
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        entityAccessRightsService.setOwnerRoleOnEntity(USER_UUID, entityId01).shouldSucceed()
        entityAccessRightsService.setWriteRoleOnEntity(USER_UUID, entityId02).shouldSucceed()
        entityAccessRightsService.getEntitiesIdsOwnedBySubject(USER_UUID)
            .shouldSucceedWith {
                assertEquals(1, it.size)
                assertEquals(entityId01, it[0])
            }
    }

    @Test
    fun `it should delete all access rights on entities`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI))
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        entityAccessRightsService.setOwnerRoleOnEntity(USER_UUID, entityId01).shouldSucceed()
        entityAccessRightsService.setWriteRoleOnEntity(USER_UUID, entityId02).shouldSucceed()
        val entitiesIds = listOf(entityId01, entityId02)
        entityAccessRightsService.deleteAllAccessRightsOnEntities(entitiesIds)
            .shouldSucceed()
        entityAccessRightsService.getAccessRightsForEntities(entitiesIds)
            .shouldSucceedWith { assertTrue(it.isEmpty()) }
    }

    @Test
    fun `it should create an entity payload from string with specificAccessPolicy`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI), AUTH_WRITE)

        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId01,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId01,
            listOf(AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }

        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId02,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId02,
            listOf(AUTH_WRITE)
        ).shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should create an entity payload from an NGSI-LD Entity with specificAccessPolicy`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)

        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId01,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId01,
            listOf(AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should allow to read an entity with an AUTH_READ specific access policy`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))

        entityAccessRightsService.canReadEntity(entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(entityId02).shouldFailWith {
            it is AccessDeniedException
        }
    }

    @Test
    fun `it should allow to read an entity with an AUTH_READ or AUTH_WRITE specific access policy`() = runTest {
        val entityId03 = "urn:ngsi-ld:Entity:03".toUri()
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))
        createEntityPayload(entityId03, setOf(BEEHIVE_IRI), AUTH_WRITE)

        entityAccessRightsService.canReadEntity(entityId01).shouldSucceed()
        entityAccessRightsService.canReadEntity(entityId02).shouldFailWith {
            it is AccessDeniedException
        }
        entityAccessRightsService.canReadEntity(entityId03).shouldSucceed()
    }

    @Test
    fun `it should allow to write an entity with an AUTH_WRITE specific access policy`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_WRITE)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI))

        entityAccessRightsService.canWriteEntity(entityId01).shouldSucceed()
        entityAccessRightsService.canWriteEntity(entityId02).shouldFailWith {
            it is AccessDeniedException
        }
    }

    @Test
    fun `it should update a specific access policy for a temporal entity`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)
        createEntityPayload(entityId02, setOf(BEEHIVE_IRI), AUTH_READ)

        entityAccessRightsService.updateSpecificAccessPolicy(
            entityId01,
            buildSapAttribute(AUTH_WRITE)
        ).shouldSucceed()

        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId01,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId01,
            listOf(AUTH_WRITE)
        ).shouldSucceedWith { assertTrue(it) }
        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId02,
            listOf(AUTH_WRITE)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should remove a specific access policy from a entity payload`() = runTest {
        createEntityPayload(entityId01, setOf(BEEHIVE_IRI), AUTH_READ)

        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId01,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertTrue(it) }
        entityAccessRightsService.removeSpecificAccessPolicy(entityId01).shouldSucceed()
        entityAccessRightsService.hasSpecificAccessPolicies(
            entityId01,
            listOf(AUTH_READ)
        ).shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should return nothing when specific access policy list is empty`() = runTest {
        entityAccessRightsService.hasSpecificAccessPolicies(entityId01, emptyList())
            .shouldSucceedWith { assertFalse(it) }
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
            entityService.createEntityPayload(
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
