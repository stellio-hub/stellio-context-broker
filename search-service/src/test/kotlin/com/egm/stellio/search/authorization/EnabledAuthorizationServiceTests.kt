package com.egm.stellio.search.authorization

import arrow.core.Either
import arrow.core.Some
import arrow.core.right
import com.egm.stellio.search.authorization.EntityAccessRights.SubjectRightInfo
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyAll
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EnabledAuthorizationService::class])
@ActiveProfiles("test")
class EnabledAuthorizationServiceTests {

    @Autowired
    private lateinit var enabledAuthorizationService: EnabledAuthorizationService

    @MockkBean
    private lateinit var subjectReferentialService: SubjectReferentialService

    @MockkBean
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    private val subjectUuid = "0768A6D5-D87B-4209-9A22-8C40A8961A79"
    private val groupUuid = "220FC854-3609-404B-BC77-F2DFE332B27B"

    private val entityId01 = "urn:ngsi-ld:Beehive:01".toUri()
    private val entityId02 = "urn:ngsi-ld:Beehive:02".toUri()

    @Test
    fun `it should return an access denied if user has no global role`() = runTest {
        coEvery { subjectReferentialService.getGlobalRoles(any()) } returns emptyList()

        enabledAuthorizationService.userIsOneOfGivenRoles(CREATION_ROLES, Some(subjectUuid))
            .shouldFail {
                assertInstanceOf(AccessDeniedException::class.java, it)
            }

        coVerify { subjectReferentialService.getGlobalRoles(eq(Some(subjectUuid))) }
    }

    @Test
    fun `it should allow an user that has one of the required roles`() = runTest {
        coEvery { subjectReferentialService.getGlobalRoles(any()) } returns listOf(Some(GlobalRole.STELLIO_CREATOR))

        enabledAuthorizationService.userIsOneOfGivenRoles(CREATION_ROLES, Some(subjectUuid))
            .shouldSucceed()

        coVerify { subjectReferentialService.getGlobalRoles(eq(Some(subjectUuid))) }
    }

    @Test
    fun `it should return an access denied if user cannot read the given entity`() = runTest {
        coEvery { entityAccessRightsService.checkHasRightOnEntity(any(), any(), any(), any()) } returns false.right()

        enabledAuthorizationService.userCanReadEntity(entityId01, Some(subjectUuid))
            .shouldFail {
                assertInstanceOf(AccessDeniedException::class.java, it)
                assertEquals("User forbidden to read entity", it.message)
            }

        coVerify {
            entityAccessRightsService.checkHasRightOnEntity(
                eq(Some(subjectUuid)),
                eq(entityId01),
                listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ),
                listOf(AccessRight.R_CAN_ADMIN, AccessRight.R_CAN_WRITE, AccessRight.R_CAN_READ)
            )
        }
    }

    @Test
    fun `it should allow an user that has the right to read an entity`() = runTest {
        coEvery { entityAccessRightsService.checkHasRightOnEntity(any(), any(), any(), any()) } returns true.right()

        enabledAuthorizationService.userCanReadEntity(entityId01, Some(subjectUuid))
            .shouldSucceed()

        coVerify {
            entityAccessRightsService.checkHasRightOnEntity(
                eq(Some(subjectUuid)),
                eq(entityId01),
                listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ),
                listOf(AccessRight.R_CAN_ADMIN, AccessRight.R_CAN_WRITE, AccessRight.R_CAN_READ)
            )
        }
    }

    @Test
    fun `it should return an access denied if user cannot update the given entity`() = runTest {
        coEvery { entityAccessRightsService.checkHasRightOnEntity(any(), any(), any(), any()) } returns false.right()

        enabledAuthorizationService.userCanUpdateEntity(entityId01, Some(subjectUuid))
            .shouldFail {
                assertInstanceOf(AccessDeniedException::class.java, it)
                assertEquals("User forbidden to modify entity", it.message)
            }

        coVerify {
            entityAccessRightsService.checkHasRightOnEntity(
                eq(Some(subjectUuid)),
                eq(entityId01),
                listOf(SpecificAccessPolicy.AUTH_WRITE),
                listOf(AccessRight.R_CAN_ADMIN, AccessRight.R_CAN_WRITE)
            )
        }
    }

    @Test
    fun `it should allow an user that has the right to update an entity`() = runTest {
        coEvery { entityAccessRightsService.checkHasRightOnEntity(any(), any(), any(), any()) } returns true.right()

        enabledAuthorizationService.userCanUpdateEntity(entityId01, Some(subjectUuid))
            .shouldSucceed()

        coVerify {
            entityAccessRightsService.checkHasRightOnEntity(
                eq(Some(subjectUuid)),
                eq(entityId01),
                listOf(SpecificAccessPolicy.AUTH_WRITE),
                listOf(AccessRight.R_CAN_ADMIN, AccessRight.R_CAN_WRITE)
            )
        }
    }

    @Test
    fun `it should return an access denied if user cannot admin the given entity`() = runTest {
        coEvery { entityAccessRightsService.checkHasRightOnEntity(any(), any(), any(), any()) } returns false.right()

        enabledAuthorizationService.userCanAdminEntity(entityId01, Some(subjectUuid))
            .shouldFail {
                assertInstanceOf(AccessDeniedException::class.java, it)
                assertEquals("User forbidden to admin entity", it.message)
            }

        coVerify {
            entityAccessRightsService.checkHasRightOnEntity(
                eq(Some(subjectUuid)),
                eq(entityId01),
                emptyList(),
                listOf(AccessRight.R_CAN_ADMIN)
            )
        }
    }

    @Test
    fun `it should allow an user that has the right to admin an entity`() = runTest {
        coEvery { entityAccessRightsService.checkHasRightOnEntity(any(), any(), any(), any()) } returns true.right()

        enabledAuthorizationService.userCanAdminEntity(entityId01, Some(subjectUuid))
            .shouldSucceed()

        coVerify {
            entityAccessRightsService.checkHasRightOnEntity(
                eq(Some(subjectUuid)),
                eq(entityId01),
                emptyList(),
                listOf(AccessRight.R_CAN_ADMIN)
            )
        }
    }

    @Test
    fun `it should create admin link for a set of entities`() = runTest {
        coEvery { entityAccessRightsService.setAdminRoleOnEntity(any(), any()) } returns Unit.right()

        enabledAuthorizationService.createAdminRights(listOf(entityId01, entityId02), Some(subjectUuid))
            .shouldSucceed()

        coVerifyAll {
            entityAccessRightsService.setAdminRoleOnEntity(eq(subjectUuid), eq(entityId01))
            entityAccessRightsService.setAdminRoleOnEntity(eq(subjectUuid), eq(entityId02))
        }
    }

    @Test
    fun `it should return a null filter is user has the stellio-admin role`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } returns true.right()

        val accessRightFilter = enabledAuthorizationService.computeAccessRightFilter(Some(subjectUuid))
        assertNull(accessRightFilter())
    }

    @Test
    fun `it should return a valid entity filter if user does not have the stellio-admin role`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } returns false.right()
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } returns listOf(subjectUuid, groupUuid).right()

        val accessRightFilter = enabledAuthorizationService.computeAccessRightFilter(Some(subjectUuid))
        assertEquals(
            """
            ( 
                (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                OR
                (entity_payload.entity_id IN (
                    SELECT entity_id
                    FROM entity_access_rights
                    WHERE subject_id IN ('$subjectUuid','$groupUuid')
                ))
            )
            """.trimIndent(),
            accessRightFilter()
        )
    }

    @Test
    fun `it should return serialized groups memberships along with a count for an admin`() = runTest {
        coEvery { subjectReferentialService.getGlobalRoles(any()) } returns listOf(Some(GlobalRole.STELLIO_ADMIN))
        coEvery {
            subjectReferentialService.getAllGroups(any(), any(), any())
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

        enabledAuthorizationService.getGroupsMemberships(0, 2, Some(subjectUuid))
            .shouldSucceedWith {
                assertEquals(2, it.first)
                it.second.forEach { jsonLdEntity ->
                    assertEquals(1, jsonLdEntity.types.size)
                    assertEquals(GROUP_TYPE, jsonLdEntity.types[0])
                    assertTrue(jsonLdEntity.id.startsWith(GROUP_ENTITY_PREFIX))
                }
            }
    }

    @Test
    fun `it should return serialized groups memberships along with a count for an user without any roles`() = runTest {
        coEvery { subjectReferentialService.getGlobalRoles(any()) } returns emptyList()
        coEvery {
            subjectReferentialService.getGroups(any(), any(), any())
        } returns listOf(
            Group(
                id = UUID.randomUUID().toString(),
                name = "Group 1"
            )
        )
        coEvery { subjectReferentialService.getCountGroups(any()) } returns Either.Right(1)

        enabledAuthorizationService.getGroupsMemberships(0, 2, Some(subjectUuid))
            .shouldSucceedWith {
                assertEquals(1, it.first)
                assertEquals(1, it.second[0].types.size)
                assertEquals(GROUP_TYPE, it.second[0].types[0])
                assertTrue(it.second[0].id.startsWith(GROUP_ENTITY_PREFIX))
            }

        coVerify {
            subjectReferentialService.getGroups(eq(Some(subjectUuid)), eq(0), eq(2))
            subjectReferentialService.getCountGroups(eq(Some(subjectUuid)))
        }
    }

    @Test
    fun `it should returned serialized access control entities with a count`() = runTest {
        coEvery {
            entityAccessRightsService.getSubjectAccessRights(any(), any(), any(), any(), any())
        } returns listOf(
            EntityAccessRights(
                id = entityId01,
                types = listOf(BEEHIVE_TYPE),
                right = AccessRight.R_CAN_WRITE
            )
        ).right()
        coEvery { entityAccessRightsService.getSubjectAccessRightsCount(any(), any(), any()) } returns Either.Right(1)
        coEvery {
            entityAccessRightsService.getAccessRightsForEntities(any(), any())
        } returns emptyMap<URI, Map<AccessRight, List<SubjectRightInfo>>>().right()

        enabledAuthorizationService.getAuthorizedEntities(
            QueryParams(
                type = BEEHIVE_TYPE,
                limit = 10,
                offset = 0,
                context = APIC_COMPOUND_CONTEXT
            ),
            context = APIC_COMPOUND_CONTEXT,
            sub = Some(subjectUuid)
        ).shouldSucceedWith {
            assertEquals(1, it.first)
            it.second.forEach { jsonLdEntity ->
                assertEquals(1, jsonLdEntity.types.size)
                assertEquals(BEEHIVE_TYPE, jsonLdEntity.types[0])
            }
        }

        coVerify {
            entityAccessRightsService.getAccessRightsForEntities(
                eq(Some(subjectUuid)),
                emptyList()
            )
        }
    }

    @Test
    fun `it should returned serialized access control entities with other rigths if user is admin`() = runTest {
        coEvery {
            entityAccessRightsService.getSubjectAccessRights(any(), any(), any(), any(), any())
        } returns listOf(
            EntityAccessRights(
                id = entityId01,
                types = listOf(BEEHIVE_TYPE),
                right = AccessRight.R_CAN_ADMIN
            ),
            EntityAccessRights(
                id = entityId02,
                types = listOf(BEEHIVE_TYPE),
                right = AccessRight.R_CAN_WRITE
            )
        ).right()
        coEvery { entityAccessRightsService.getSubjectAccessRightsCount(any(), any(), any()) } returns Either.Right(1)
        coEvery {
            entityAccessRightsService.getAccessRightsForEntities(any(), any())
        } returns mapOf(
            entityId01 to mapOf(
                AccessRight.R_CAN_WRITE to listOf(
                    SubjectRightInfo(
                        "urn:ngsi-ld:User:01".toUri(),
                        mapOf("kind" to "User", "username" to "stellio")
                    )
                )
            )
        ).right()

        enabledAuthorizationService.getAuthorizedEntities(
            QueryParams(
                type = BEEHIVE_TYPE,
                limit = 10,
                offset = 0,
                context = APIC_COMPOUND_CONTEXT
            ),
            context = APIC_COMPOUND_CONTEXT,
            sub = Some(subjectUuid)
        ).shouldSucceedWith {
            assertEquals(1, it.first)
            assertEquals(2, it.second.size)

            val jsonLdEntityWithOtherRights = it.second.find { it.id == entityId01.toString() }!!
            assertEquals(4, jsonLdEntityWithOtherRights.members.size)
            assertTrue(jsonLdEntityWithOtherRights.members.containsKey(AUTH_REL_CAN_WRITE))
        }

        coVerify {
            entityAccessRightsService.getAccessRightsForEntities(
                eq(Some(subjectUuid)),
                listOf(entityId01)
            )
        }
    }
}
