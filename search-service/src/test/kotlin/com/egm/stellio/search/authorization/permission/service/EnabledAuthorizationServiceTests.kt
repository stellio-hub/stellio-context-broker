package com.egm.stellio.search.authorization.permission.service

import arrow.core.Either
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.authorization.subject.USER_UUID
import com.egm.stellio.search.authorization.subject.model.Group
import com.egm.stellio.search.authorization.subject.model.User
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.AUTHZ_TEST_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_USERNAME
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.USER_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.CREATION_ROLES
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EnabledAuthorizationService::class])
@ActiveProfiles("test")
class EnabledAuthorizationServiceTests {

    @Autowired
    private lateinit var enabledAuthorizationService: EnabledAuthorizationService

    @MockkBean
    private lateinit var subjectReferentialService: SubjectReferentialService

    @MockkBean
    private lateinit var permissionService: PermissionService

    private val subjectUuid = "0768A6D5-D87B-4209-9A22-8C40A8961A79"
    private val groupUuid = "220FC854-3609-404B-BC77-F2DFE332B27B"

    private val entityId01 = "urn:ngsi-ld:Beehive:01".toUri()
    private val entityId02 = "urn:ngsi-ld:Beehive:02".toUri()

    @Test
    fun `it should return false if user has no global role`() = runTest {
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasOneOfGlobalRoles(any(), any()) } returns false.right()

        enabledAuthorizationService.userHasOneOfGivenRoles(CREATION_ROLES)
            .shouldSucceedWith { assertFalse(it) }

        coVerify { subjectReferentialService.getSubjectAndGroupsUUID() }
        coVerify { subjectReferentialService.hasOneOfGlobalRoles(eq(listOf(subjectUuid)), eq(CREATION_ROLES)) }
    }

    @Test
    fun `it should return true if user has one of the required roles`() = runTest {
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasOneOfGlobalRoles(any(), any()) } returns true.right()

        enabledAuthorizationService.userHasOneOfGivenRoles(CREATION_ROLES)
            .shouldSucceedWith { assertTrue(it) }

        coVerify { subjectReferentialService.getSubjectAndGroupsUUID() }
        coVerify { subjectReferentialService.hasOneOfGlobalRoles(eq(listOf(subjectUuid)), eq(CREATION_ROLES)) }
    }

    @Test
    fun `userCanReadEntity should return an access denied if user does not have the rights`() = runTest {
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns false.right()

        enabledAuthorizationService.userCanReadEntity(entityId01)
            .shouldFail {
                assertInstanceOf(AccessDeniedException::class.java, it)
                assertEquals("User forbidden to read entity", it.message)
            }

        coVerify {
            permissionService.checkHasPermissionOnEntity(
                eq(entityId01),
                Action.READ
            )
        }
    }

    @Test
    fun `it should allow an user that has the right to read an entity`() = runTest {
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns true.right()

        enabledAuthorizationService.userCanReadEntity(entityId01)
            .shouldSucceed()

        coVerify {
            permissionService.checkHasPermissionOnEntity(
                eq(entityId01),
                Action.READ
            )
        }
    }

    @Test
    fun `it should return an access denied if user cannot update the given entity`() = runTest {
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns false.right()

        enabledAuthorizationService.userCanUpdateEntity(entityId01)
            .shouldFail {
                assertInstanceOf(AccessDeniedException::class.java, it)
                assertEquals("User forbidden to modify entity", it.message)
            }

        coVerify {
            permissionService.checkHasPermissionOnEntity(
                eq(entityId01),
                Action.WRITE
            )
        }
    }

    @Test
    fun `it should allow an user that has the right to update an entity`() = runTest {
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns true.right()

        enabledAuthorizationService.userCanUpdateEntity(entityId01)
            .shouldSucceed()

        coVerify {
            permissionService.checkHasPermissionOnEntity(
                eq(entityId01),
                Action.WRITE
            )
        }
    }

    @Test
    fun `it should return an access denied if user cannot admin the given entity`() = runTest {
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns false.right()

        enabledAuthorizationService.userCanAdminEntity(entityId01)
            .shouldFail {
                assertInstanceOf(AccessDeniedException::class.java, it)
                assertEquals("User forbidden to admin entity", it.message)
            }

        coVerify {
            permissionService.checkHasPermissionOnEntity(
                eq(entityId01),
                Action.ADMIN
            )
        }
    }

    @Test
    fun `it should allow an user that has the right to admin an entity`() = runTest {
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns true.right()

        enabledAuthorizationService.userCanAdminEntity(entityId01)
            .shouldSucceed()

        coVerify {
            permissionService.checkHasPermissionOnEntity(
                eq(entityId01),
                Action.ADMIN
            )
        }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `it should create owner link for a set of entities`() = runTest {
        coEvery { permissionService.create(any()) } returns Unit.right()

        enabledAuthorizationService.createOwnerRights(listOf(entityId01, entityId02))
            .shouldSucceed()

        coVerifyAll {
            permissionService.create(
                match {
                    it.target == TargetAsset(entityId01) &&
                        it.assignee == subjectUuid &&
                        it.action == Action.OWN
                }
            )
            permissionService.create(
                match {
                    it.target == TargetAsset(entityId02) &&
                        it.assignee == subjectUuid &&
                        it.action == Action.OWN
                }
            )
        }
    }

    @Test
    fun `it should return a null filter is user has the stellio-admin role`() = runTest {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid)) } returns true.right()

        val accessRightFilter = enabledAuthorizationService.getAccessRightFilter()

        assertNull(accessRightFilter)
    }

    @Test
    fun `it should return a valid entity filter if user does not have the stellio-admin role`() = runTest {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(subjectUuid, groupUuid).right()
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns false.right()
        coEvery { permissionService.buildAsRightOnEntityFilter(any(), any()) } returns "entity filter"
        coEvery { permissionService.buildCandidatePermissionWithStatement(any(), any()) } returns "admin clause"

        val accessRightFilter = enabledAuthorizationService.getAccessRightFilter()
        val adminPermissionWithClause = enabledAuthorizationService.getAdminPermissionWithClause()

        assertEquals(
            "entity filter",
            accessRightFilter
        )
        assertEquals(
            "admin clause",
            adminPermissionWithClause
        )
        coVerify { subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid, groupUuid)) }
    }

    @Test
    fun `get groups memberships should return data along with a count for an admin`() = runTest {
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasOneOfGlobalRoles(any(), any()) } returns true.right()
        coEvery {
            subjectReferentialService.getAllGroups(any(), any())
        } returns listOf(
            Group(
                id = UUID.randomUUID().toString(),
                name = "Group 1"
            ),
            Group(
                id = UUID.randomUUID().toString(),
                name = "Group 2"
            )
        )
        coEvery { subjectReferentialService.getCountAllGroups() } returns Either.Right(2)

        enabledAuthorizationService.getGroupsMemberships(0, 2, AUTHZ_TEST_COMPOUND_CONTEXTS)
            .shouldSucceedWith {
                assertEquals(2, it.first)
                it.second.forEach { jsonLdEntity ->
                    assertEquals(1, jsonLdEntity.types.size)
                    assertEquals(GROUP_TYPE, jsonLdEntity.types[0])
                    assertTrue(jsonLdEntity.id.toString().startsWith(GROUP_ENTITY_PREFIX))
                }
            }
    }

    @Test
    fun `get groups memberships should return data along with a count for an user without any roles`() = runTest {
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasOneOfGlobalRoles(any(), any()) } returns false.right()
        coEvery {
            subjectReferentialService.getGroups(any(), any())
        } returns listOf(
            Group(
                id = UUID.randomUUID().toString(),
                name = "Group 1"
            )
        )
        coEvery { subjectReferentialService.getCountGroups() } returns Either.Right(1)

        enabledAuthorizationService.getGroupsMemberships(0, 2, AUTHZ_TEST_COMPOUND_CONTEXTS)
            .shouldSucceedWith {
                assertEquals(1, it.first)
                assertEquals(1, it.second[0].types.size)
                assertEquals(GROUP_TYPE, it.second[0].types[0])
                assertTrue(it.second[0].id.toString().startsWith(GROUP_ENTITY_PREFIX))
            }

        coVerify {
            subjectReferentialService.getGroups(eq(0), eq(2))
            subjectReferentialService.getCountGroups()
        }
    }

    @Test
    fun `get users should return data along with a count for an admin`() = runTest {
        coEvery {
            subjectReferentialService.getUsers(any(), any())
        } returns listOf(
            User(
                id = UUID.randomUUID().toString(),
                username = "Username 1",
                givenName = "Given Name 1",
                familyName = "Family Name 1",
                subjectInfo = mapOf("profile" to "stellio user")
            ),
            User(
                id = UUID.randomUUID().toString(),
                username = "Username 2",
                subjectInfo = mapOf("profile" to "stellio user")
            )
        )
        coEvery { subjectReferentialService.getUsersCount() } returns Either.Right(2)

        enabledAuthorizationService.getUsers(0, 2, AUTHZ_TEST_COMPOUND_CONTEXTS)
            .shouldSucceedWith {
                assertEquals(2, it.first)
                it.second.forEach { jsonLdEntity ->
                    assertEquals(1, jsonLdEntity.types.size)
                    assertEquals(USER_TYPE, jsonLdEntity.types[0])
                    assertTrue(jsonLdEntity.id.toString().startsWith(USER_ENTITY_PREFIX))
                    assertTrue(jsonLdEntity.members.containsKey(AUTH_PROP_USERNAME))
                }
            }
    }
}
