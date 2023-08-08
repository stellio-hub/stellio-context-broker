package com.egm.stellio.search.authorization

import arrow.core.Some
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.GlobalRole.STELLIO_ADMIN
import com.egm.stellio.shared.util.GlobalRole.STELLIO_CREATOR
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class SubjectReferentialServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var subjectReferentialService: SubjectReferentialService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val subjectUuid = "0768A6D5-D87B-4209-9A22-8C40A8961A79"
    private val serviceAccountUuid = "3CB80121-B2D6-4F76-BE97-B459CCC3AF72"
    private val groupUuid = "52A916AB-19E6-4D3B-B629-936BC8E5B640"

    @AfterEach
    fun clearSubjectReferentialTable() {
        r2dbcEntityTemplate.delete(SubjectReferential::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should persist a subject referential`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential)
            .fold({
                fail("it should have created a subject referential")
            }, {})
    }

    @Test
    fun `it should retrieve a subject referential`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                assertEquals(subjectUuid, it.subjectId)
                assertEquals(SubjectType.USER, it.subjectType)
                assertEquals(listOf(STELLIO_ADMIN), it.globalRoles)
            }
    }

    @Test
    fun `it should retrieve a subject referential with subject info`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = getSubjectInfoForUser("stellio"),
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential).shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                val subjectInfo = it.getSubjectInfoValue()
                assertTrue(subjectInfo.containsKey("username"))
                assertEquals("stellio", subjectInfo["username"])
            }
    }

    @Test
    fun `it should retrieve UUIDs from user and groups memberships`() = runTest {
        val groupsUuids = List(3) { UUID.randomUUID().toString() }
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            groupsMemberships = groupsUuids
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
            .shouldSucceedWith {
                assertEquals(4, it.size)
                assertTrue(it.containsAll(groupsUuids.plus(subjectUuid)))
            }
    }

    @Test
    fun `it should retrieve UUIDs from user when it has no groups memberships`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
            .shouldSucceedWith {
                assertEquals(1, it.size)
                assertTrue(it.contains(subjectUuid))
            }
    }

    @Test
    fun `it should retrieve UUIDs from client and service account`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.CLIENT,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            serviceAccountId = serviceAccountUuid
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
            .shouldSucceedWith {
                assertEquals(2, it.size)
                assertTrue(it.containsAll(listOf(serviceAccountUuid, subjectUuid)))
            }
    }

    @Test
    fun `it should retrieve UUIDs from client, service account and groups memberships`() = runTest {
        val groupsUuids = List(2) { UUID.randomUUID().toString() }
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.CLIENT,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            serviceAccountId = serviceAccountUuid,
            groupsMemberships = groupsUuids
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
            .shouldSucceedWith {
                assertEquals(4, it.size)
                assertTrue(it.containsAll(groupsUuids.plus(serviceAccountUuid).plus(subjectUuid)))
            }
    }

    @Test
    fun `it should get the groups memberships of an user`() = runTest {
        val allGroupsUuids = List(3) {
            val groupUuid = UUID.randomUUID().toString()
            subjectReferentialService.create(
                SubjectReferential(
                    subjectId = groupUuid,
                    subjectType = SubjectType.GROUP,
                    subjectInfo = getSubjectInfoForGroup(groupUuid)
                )
            )
            groupUuid
        }
        val userGroupsUuids = allGroupsUuids.subList(0, 2)
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            groupsMemberships = userGroupsUuids
        )

        subjectReferentialService.create(subjectReferential)

        val groups = subjectReferentialService.getGroups(Some(subjectUuid), 0, 10)

        assertEquals(2, groups.size)
        groups.forEach {
            assertTrue(it.id in userGroupsUuids)
            assertTrue(it.name in userGroupsUuids)
            assertTrue(it.isMember)
        }

        subjectReferentialService.getCountGroups(Some(subjectUuid))
            .shouldSucceedWith { assertEquals(2, it) }
    }

    @Test
    fun `it should get all groups for an admin`() = runTest {
        val allGroupsUuids = List(3) {
            val groupUuid = UUID.randomUUID().toString()
            subjectReferentialService.create(
                SubjectReferential(
                    subjectId = groupUuid,
                    subjectType = SubjectType.GROUP,
                    subjectInfo = getSubjectInfoForGroup(groupUuid)
                )
            )
            groupUuid
        }
        val userGroupsUuids = allGroupsUuids.subList(0, 2)
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            groupsMemberships = userGroupsUuids
        )

        subjectReferentialService.create(subjectReferential)

        val groups = subjectReferentialService.getAllGroups(Some(subjectUuid), 0, 10)

        assertEquals(3, groups.size)
        groups.forEach {
            assertTrue(it.id in allGroupsUuids)
            assertTrue(it.name in allGroupsUuids)
            if (it.id in userGroupsUuids)
                assertTrue(it.isMember)
            else
                assertFalse(it.isMember)
        }

        subjectReferentialService.getCountAllGroups()
            .shouldSucceedWith { assertEquals(3, it) }
    }

    @Test
    fun `it should get all users`() = runTest {
        val allUsersUuids = List(3) {
            val userUuid = UUID.randomUUID().toString()
            subjectReferentialService.create(
                SubjectReferential(
                    subjectId = userUuid,
                    subjectType = SubjectType.USER,
                    subjectInfo = getSubjectInfoForUser(userUuid)
                )
            )
            userUuid
        }

        val users = subjectReferentialService.getUsers(0, 10)

        assertEquals(3, users.size)
        users.forEach {
            assertTrue(it.id in allUsersUuids)
            assertNotNull(it.username)
            assertNull(it.givenName)
            assertNull(it.familyName)
        }

        subjectReferentialService.getUsersCount()
            .shouldSucceedWith { assertEquals(3, it) }
    }

    @Test
    fun `it should get an user with all available information`() = runTest {
        val userUuid = UUID.randomUUID().toString()
        subjectReferentialService.create(
            SubjectReferential(
                subjectId = userUuid,
                subjectType = SubjectType.USER,
                subjectInfo = Json.of(
                    """
                    { 
                      "type": "Property",
                      "value": { 
                        "username": "username", 
                        "givenName": "givenName", 
                        "familyName": "familyName",
                        "profile": "stellio user"
                      }
                    }
                    """.trimIndent()
                )
            )
        )

        val users = subjectReferentialService.getUsers(0, 10)

        assertEquals(1, users.size)
        val user = users[0]
        assertEquals(userUuid, user.id)
        assertEquals("username", user.username)
        assertEquals("givenName", user.givenName)
        assertEquals("familyName", user.familyName)
        assertEquals(4, user.subjectInfo.size)
        assertTrue(user.subjectInfo.containsKey("profile"))
    }

    @Test
    fun `it should update the global role of a subject`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            subjectType = SubjectType.USER
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.setGlobalRoles(subjectUuid, listOf(STELLIO_ADMIN))
        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                assertEquals(listOf(STELLIO_ADMIN), it.globalRoles)
            }

        subjectReferentialService.resetGlobalRoles(subjectUuid)
        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                assertNull(it.globalRoles)
            }
    }

    @Test
    fun `it should find if an user is a stellio admin or not`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid))
            .shouldSucceedWith {
                assertTrue(it)
            }

        subjectReferentialService.resetGlobalRoles(subjectUuid)

        subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid))
            .shouldSucceedWith {
                assertFalse(it)
            }

        subjectReferentialService.setGlobalRoles(subjectUuid, listOf(STELLIO_ADMIN, STELLIO_CREATOR))

        subjectReferentialService.hasStellioAdminRole(listOf(subjectUuid))
            .shouldSucceedWith {
                assertTrue(it)
            }
    }

    @Test
    fun `it should add a group membership to an user`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )

        subjectReferentialService.create(userAccessRights)

        subjectReferentialService.addGroupMembershipToUser(subjectUuid, groupUuid)
            .shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                assertThat(it.groupsMemberships).containsAll(listOf(groupUuid))
            }
    }

    @Test
    fun `it should add a group membership to an user inside an existing list`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )

        subjectReferentialService.create(userAccessRights)
        subjectReferentialService.addGroupMembershipToUser(subjectUuid, groupUuid)

        val newGroupUuid = UUID.randomUUID().toString()

        subjectReferentialService.addGroupMembershipToUser(subjectUuid, newGroupUuid)
            .shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                assertThat(it.groupsMemberships).containsAll(listOf(groupUuid, newGroupUuid))
            }
    }

    @Test
    fun `it should remove a group membership to an user`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )

        subjectReferentialService.create(userAccessRights)
        subjectReferentialService.addGroupMembershipToUser(subjectUuid, groupUuid)

        subjectReferentialService.removeGroupMembershipToUser(subjectUuid, groupUuid)
            .shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                assertTrue(it.groupsMemberships?.isEmpty() ?: false)
            }
    }

    @Test
    fun `it should add a service account id to a client`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )

        subjectReferentialService.create(userAccessRights)

        val serviceAccountId = UUID.randomUUID().toString()

        subjectReferentialService.addServiceAccountIdToClient(subjectUuid, serviceAccountId)
            .shouldSucceed()

        subjectReferentialService.retrieve(serviceAccountId)
            .shouldSucceedWith {
                assertEquals(serviceAccountId, it.serviceAccountId)
            }
    }

    @Test
    fun `it should update an existing subject info for an user`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = getSubjectInfoForUser("stellio")
        )

        subjectReferentialService.create(subjectReferential).shouldSucceed()

        subjectReferentialService.updateSubjectInfo(subjectUuid, Pair("username", "stellio v2"))
            .shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                val newValue = it.getSubjectInfoValue()
                assertTrue(newValue.containsKey("username"))
                assertEquals("stellio v2", newValue["username"])
            }
    }

    @Test
    fun `it should add a subject info for an user`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = getSubjectInfoForUser("stellio")
        )

        subjectReferentialService.create(subjectReferential).shouldSucceed()

        subjectReferentialService.updateSubjectInfo(subjectUuid, Pair("givenName", "Blaise"))
            .shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                val newValue = it.getSubjectInfoValue()
                assertTrue(newValue.containsKey("username"))
                assertEquals("stellio", newValue["username"])
                assertTrue(newValue.containsKey("givenName"))
                assertEquals("Blaise", newValue["givenName"])
            }
    }

    @Test
    fun `it should delete a subject referential`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(userAccessRights)

        subjectReferentialService.delete(subjectUuid)
            .shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .fold({
                assertInstanceOf(AccessDeniedException::class.java, it)
            }, {
                fail("it should have returned a AccessDeniedException exception")
            })
    }

    @Test
    fun `it should return false when user does not have the requested roles`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )
        subjectReferentialService.create(subjectReferential)
        subjectReferentialService.hasOneOfGlobalRoles(listOf(subjectUuid), CREATION_ROLES)
            .shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `it should return true when user has admin roles`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_ADMIN)
        )
        subjectReferentialService.create(subjectReferential)
        subjectReferentialService.hasOneOfGlobalRoles(listOf(subjectUuid), ADMIN_ROLES)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should return true when user has creation roles`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_CREATOR, STELLIO_ADMIN)
        )
        subjectReferentialService.create(subjectReferential)
        subjectReferentialService.hasOneOfGlobalRoles(listOf(subjectUuid), CREATION_ROLES)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `it should return true when user has a global role inherited from a group`() = runTest {
        subjectReferentialService.create(
            SubjectReferential(
                subjectId = subjectUuid,
                subjectType = SubjectType.USER,
                subjectInfo = EMPTY_JSON_PAYLOAD,
                globalRoles = emptyList(),
                groupsMemberships = listOf(groupUuid)
            )
        )
        subjectReferentialService.create(
            SubjectReferential(
                subjectId = groupUuid,
                subjectType = SubjectType.GROUP,
                subjectInfo = getSubjectInfoForGroup(groupUuid),
                globalRoles = listOf(STELLIO_CREATOR, STELLIO_ADMIN)
            )
        )
        val uuids = subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid)).shouldSucceedAndResult()
        subjectReferentialService.hasOneOfGlobalRoles(uuids, CREATION_ROLES)
            .shouldSucceedWith { assertTrue(it) }
    }
}
