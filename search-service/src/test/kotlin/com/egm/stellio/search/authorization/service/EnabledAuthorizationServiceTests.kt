package com.egm.stellio.search.authorization.service

import arrow.core.Either
import arrow.core.Some
import arrow.core.right
import com.egm.stellio.search.authorization.model.EntityAccessRights
import com.egm.stellio.search.authorization.model.EntityAccessRights.SubjectRightInfo
import com.egm.stellio.search.authorization.model.Group
import com.egm.stellio.search.authorization.model.User
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AUTHZ_TEST_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AccessRight.CAN_ADMIN
import com.egm.stellio.shared.util.AccessRight.CAN_READ
import com.egm.stellio.shared.util.AccessRight.CAN_WRITE
import com.egm.stellio.shared.util.AccessRight.IS_OWNER
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_USERNAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_WRITE
import com.egm.stellio.shared.util.AuthContextModel.USER_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.CREATION_ROLES
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
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
import java.net.URI
import java.util.UUID

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
    fun `it should return false if user has no global role`() = runTest {
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID(any()) } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasOneOfGlobalRoles(any(), any()) } returns false.right()

        enabledAuthorizationService.userHasOneOfGivenRoles(CREATION_ROLES, Some(subjectUuid))
            .shouldSucceedWith { assertFalse(it) }

        coVerify { subjectReferentialService.getSubjectAndGroupsUUID(eq(Some(subjectUuid))) }
        coVerify { subjectReferentialService.hasOneOfGlobalRoles(eq(listOf(subjectUuid)), eq(CREATION_ROLES)) }
    }

    @Test
    fun `it should return true if user has one of the required roles`() = runTest {
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID(any()) } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasOneOfGlobalRoles(any(), any()) } returns true.right()

        enabledAuthorizationService.userHasOneOfGivenRoles(CREATION_ROLES, Some(subjectUuid))
            .shouldSucceedWith { assertTrue(it) }

        coVerify { subjectReferentialService.getSubjectAndGroupsUUID(eq(Some(subjectUuid))) }
        coVerify { subjectReferentialService.hasOneOfGlobalRoles(eq(listOf(subjectUuid)), eq(CREATION_ROLES)) }
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
                listOf(AUTH_WRITE, AUTH_READ),
                listOf(IS_OWNER, CAN_ADMIN, CAN_WRITE, CAN_READ)
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
                listOf(AUTH_WRITE, AUTH_READ),
                listOf(IS_OWNER, CAN_ADMIN, CAN_WRITE, CAN_READ)
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
                listOf(AUTH_WRITE),
                listOf(IS_OWNER, CAN_ADMIN, CAN_WRITE)
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
                listOf(AUTH_WRITE),
                listOf(IS_OWNER, CAN_ADMIN, CAN_WRITE)
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
                listOf(IS_OWNER, CAN_ADMIN)
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
                listOf(IS_OWNER, CAN_ADMIN)
            )
        }
    }

    @Test
    fun `it should create owner link for a set of entities`() = runTest {
        coEvery { entityAccessRightsService.setOwnerRoleOnEntity(any(), any()) } returns Unit.right()

        enabledAuthorizationService.createOwnerRights(listOf(entityId01, entityId02), Some(subjectUuid))
            .shouldSucceed()

        coVerifyAll {
            entityAccessRightsService.setOwnerRoleOnEntity(eq(subjectUuid), eq(entityId01))
            entityAccessRightsService.setOwnerRoleOnEntity(eq(subjectUuid), eq(entityId02))
        }
    }

    @Test
    fun `it should return a null filter is user has the stellio-admin role`() = runTest {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid)) } returns true.right()

        val accessRightFilter = enabledAuthorizationService.computeAccessRightFilter(Some(subjectUuid))
        assertNull(accessRightFilter())
    }

    @Test
    fun `it should return a valid entity filter if user does not have the stellio-admin role`() = runTest {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } returns listOf(subjectUuid, groupUuid).right()
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns false.right()

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

        coVerify { subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid, groupUuid)) }
    }

    @Test
    fun `it should return serialized groups memberships along with a count for an admin`() = runTest {
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID(any()) } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasOneOfGlobalRoles(any(), any()) } returns true.right()
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

        enabledAuthorizationService.getGroupsMemberships(0, 2, AUTHZ_TEST_COMPOUND_CONTEXTS, Some(subjectUuid))
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
    fun `it should return serialized groups memberships along with a count for an user without any roles`() = runTest {
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID(any()) } returns listOf(subjectUuid).right()
        coEvery { subjectReferentialService.hasOneOfGlobalRoles(any(), any()) } returns false.right()
        coEvery {
            subjectReferentialService.getGroups(any(), any(), any())
        } returns listOf(
            Group(
                id = UUID.randomUUID().toString(),
                name = "Group 1"
            )
        )
        coEvery { subjectReferentialService.getCountGroups(any()) } returns Either.Right(1)

        enabledAuthorizationService.getGroupsMemberships(0, 2, AUTHZ_TEST_COMPOUND_CONTEXTS, Some(subjectUuid))
            .shouldSucceedWith {
                assertEquals(1, it.first)
                assertEquals(1, it.second[0].types.size)
                assertEquals(GROUP_TYPE, it.second[0].types[0])
                assertTrue(it.second[0].id.toString().startsWith(GROUP_ENTITY_PREFIX))
            }

        coVerify {
            subjectReferentialService.getGroups(eq(Some(subjectUuid)), eq(0), eq(2))
            subjectReferentialService.getCountGroups(eq(Some(subjectUuid)))
        }
    }

    @Test
    fun `it should return serialized users along with a count for an admin`() = runTest {
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

    @Test
    fun `it should returned serialized access control entities with a count`() = runTest {
        coEvery {
            entityAccessRightsService.getSubjectAccessRights(any(), any(), any(), any())
        } returns listOf(
            EntityAccessRights(
                id = entityId01,
                types = listOf(BEEHIVE_TYPE),
                right = CAN_WRITE
            )
        ).right()
        coEvery {
            entityAccessRightsService.getSubjectAccessRightsCount(any(), any(), any(), any(), any())
        } returns Either.Right(1)
        coEvery {
            entityAccessRightsService.getAccessRightsForEntities(any(), any())
        } returns emptyMap<URI, Map<AccessRight, List<SubjectRightInfo>>>().right()

        enabledAuthorizationService.getAuthorizedEntities(
            EntitiesQueryFromGet(
                typeSelection = BEEHIVE_TYPE,
                paginationQuery = PaginationQuery(limit = 10, offset = 0),
                contexts = APIC_COMPOUND_CONTEXTS
            ),
            contexts = APIC_COMPOUND_CONTEXTS,
            includeDeleted = false,
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
            entityAccessRightsService.getSubjectAccessRights(any(), any(), any(), any())
        } returns listOf(
            EntityAccessRights(
                id = entityId01,
                types = listOf(BEEHIVE_TYPE),
                right = CAN_ADMIN
            ),
            EntityAccessRights(
                id = entityId02,
                types = listOf(BEEHIVE_TYPE),
                right = CAN_WRITE
            )
        ).right()
        coEvery {
            entityAccessRightsService.getSubjectAccessRightsCount(any(), any(), any(), any(), any())
        } returns Either.Right(1)
        coEvery {
            entityAccessRightsService.getAccessRightsForEntities(any(), any())
        } returns mapOf(
            entityId01 to mapOf(
                CAN_WRITE to listOf(
                    SubjectRightInfo(
                        "urn:ngsi-ld:User:01".toUri(),
                        mapOf("kind" to "User", "username" to "stellio")
                    )
                )
            )
        ).right()

        enabledAuthorizationService.getAuthorizedEntities(
            EntitiesQueryFromGet(
                typeSelection = BEEHIVE_TYPE,
                paginationQuery = PaginationQuery(limit = 10, offset = 0),
                contexts = APIC_COMPOUND_CONTEXTS
            ),
            includeDeleted = false,
            contexts = APIC_COMPOUND_CONTEXTS,
            sub = Some(subjectUuid)
        ).shouldSucceedWith {
            assertEquals(1, it.first)
            assertEquals(2, it.second.size)

            val expandedEntityWithOtherRights = it.second.find { it.id == entityId01 }!!
            assertEquals(4, expandedEntityWithOtherRights.members.size)
            assertTrue(expandedEntityWithOtherRights.members.containsKey(AUTH_REL_CAN_WRITE))
        }

        coVerify {
            entityAccessRightsService.getAccessRightsForEntities(
                eq(Some(subjectUuid)),
                listOf(entityId01)
            )
        }
    }

    @Test
    fun `it should return serialized access control entities with other rigths if user is owner`() = runTest {
        coEvery {
            entityAccessRightsService.getSubjectAccessRights(any(), any(), any(), any())
        } returns listOf(EntityAccessRights(id = entityId01, types = listOf(BEEHIVE_TYPE), right = IS_OWNER)).right()
        coEvery {
            entityAccessRightsService.getSubjectAccessRightsCount(any(), any(), any(), any(), any())
        } returns Either.Right(1)
        coEvery {
            entityAccessRightsService.getAccessRightsForEntities(any(), any())
        } returns mapOf(
            entityId01 to mapOf(
                CAN_WRITE to listOf(
                    SubjectRightInfo(
                        "urn:ngsi-ld:User:01".toUri(),
                        mapOf("kind" to "User", "username" to "stellio")
                    )
                ),
                CAN_ADMIN to listOf(
                    SubjectRightInfo(
                        "urn:ngsi-ld:User:02".toUri(),
                        mapOf("kind" to "User", "username" to "jean.dupont")
                    )
                )
            )
        ).right()

        enabledAuthorizationService.getAuthorizedEntities(
            EntitiesQueryFromGet(
                typeSelection = BEEHIVE_TYPE,
                paginationQuery = PaginationQuery(limit = 10, offset = 0),
                contexts = APIC_COMPOUND_CONTEXTS
            ),
            includeDeleted = false,
            contexts = AUTHZ_TEST_COMPOUND_CONTEXTS,
            sub = Some(subjectUuid)
        ).shouldSucceedWith {
            assertEquals(1, it.first)
            assertEquals(1, it.second.size)

            val expandedEntityWithOtherRights = it.second.first()
            val compactedEntity = compactEntity(expandedEntityWithOtherRights, AUTHZ_TEST_COMPOUND_CONTEXTS)

            val expectedEntity = """
                {
                    "id": "urn:ngsi-ld:Beehive:01",
                    "type": "https://ontology.eglobalmark.com/apic#BeeHive",
                    "right": {
                        "type": "Property",
                        "value": "isOwner"
                    },
                    "canAdmin": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:User:02",
                        "datasetId": "urn:ngsi-ld:Dataset:02",
                        "subjectInfo": {
                            "type": "Property",
                            "value": {
                                "kind": "User",
                                "username": "jean.dupont"
                            }
                        }
                    },
                    "canWrite": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:User:01",
                        "datasetId": "urn:ngsi-ld:Dataset:01",
                        "subjectInfo": {
                            "type": "Property",
                            "value": {
                                "kind": "User",
                                "username": "stellio"
                            }
                        }
                    },
                    "@context": "http://localhost:8093/jsonld-contexts/authorization-compound.jsonld"
                }
            """.trimIndent()
            assertJsonPayloadsAreEqual(expectedEntity, serializeObject(compactedEntity))
        }

        coVerify {
            entityAccessRightsService.getAccessRightsForEntities(
                eq(Some(subjectUuid)),
                listOf(entityId01)
            )
        }
    }
}
