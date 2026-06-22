package com.egm.stellio.search.authorization.subject.service

import com.egm.stellio.search.authorization.subject.GROUP_UUID
import com.egm.stellio.search.authorization.subject.SERVICE_ACCOUNT_UUID
import com.egm.stellio.search.authorization.subject.USER_UUID
import com.egm.stellio.search.authorization.subject.getSubjectInfoForClient
import com.egm.stellio.search.authorization.subject.getSubjectInfoForGroup
import com.egm.stellio.search.authorization.subject.getSubjectInfoForUser
import com.egm.stellio.search.authorization.subject.model.SubjectReferential
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.ADMIN_ROLES
import com.egm.stellio.shared.util.AuthContextModel.GENERIC_SUBJECTS
import com.egm.stellio.shared.util.CREATION_ROLES
import com.egm.stellio.shared.util.GlobalRole.STELLIO_ADMIN
import com.egm.stellio.shared.util.GlobalRole.STELLIO_CREATOR
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import java.util.UUID

@SpringBootTest
@ActiveProfiles("test")
class SubjectReferentialServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var subjectReferentialService: SubjectReferentialService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @AfterEach
    fun clearSubjectReferentialTable() {
        r2dbcEntityTemplate.delete<SubjectReferential>()
            .all()
            .block()
    }

    @Test
    fun `create should persist a subject referential`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
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
    fun `create should persist a subject referential for a non-existing client`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = SERVICE_ACCOUNT_UUID,
            subjectType = SubjectType.CLIENT,
            subjectInfo = getSubjectInfoForClient("client-id", "kc-id")
        )

        subjectReferentialService.create(subjectReferential)
            .fold({
                fail("it should have created a subject referential for the client")
            }, {})
    }

    @Test
    fun `create should upsert a subject referential for an existing client`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = SERVICE_ACCOUNT_UUID,
            subjectType = SubjectType.CLIENT,
            subjectInfo = getSubjectInfoForClient("client-id-updated", "kc-id-updated")
        )

        subjectReferentialService.create(subjectReferential)
            .fold({
                fail("it should have created a subject referential for the client")
            }, {})

        subjectReferentialService.retrieve(SERVICE_ACCOUNT_UUID)
            .shouldSucceedWith {
                val subjectInfo = it.getSubjectInfoValue()
                assertTrue(subjectInfo.containsKey("clientId"))
                assertEquals("client-id-updated", subjectInfo["clientId"])
                assertTrue(subjectInfo.containsKey("internalClientId"))
                assertEquals("kc-id-updated", subjectInfo["internalClientId"])
            }
    }

    @Test
    fun `retrieve should return a SubjectReferential for a known subject`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                assertEquals(USER_UUID, it.subjectId)
                assertEquals(SubjectType.USER, it.subjectType)
                assertEquals(listOf(STELLIO_ADMIN), it.globalRoles)
            }
    }

    @Test
    fun `retrieve should include SubjectInfo in the returned referential`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = getSubjectInfoForUser("stellio"),
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential).shouldSucceed()

        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                val subjectInfo = it.getSubjectInfoValue()
                assertTrue(subjectInfo.containsKey("username"))
                assertEquals("stellio", subjectInfo["username"])
            }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User", groupsUUIDs = ["group-uuid-1", "group-uuid-2"])
    fun `getCurrentSubjectClaims should retrieve UUID from subject and groups memberships`() = runTest {
        val groups = listOf("group-uuid-1", "group-uuid-2")

        subjectReferentialService.getCurrentSubjectClaims()
            .shouldSucceedWith {
                assertEquals(5, it.size)
                assertThat(it).containsAll(groups.plus(USER_UUID).plus(GENERIC_SUBJECTS))
            }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `getCurrentSubjectClaims should retrieve UUIDs from subject when it has no groups memberships`() = runTest {
        subjectReferentialService.getCurrentSubjectClaims()
            .shouldSucceedWith {
                assertEquals(3, it.size)
                assertTrue(it.containsAll(listOf(USER_UUID).plus(GENERIC_SUBJECTS)))
            }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User", roles = ["stellio-admin", "test-role"])
    fun `getCurrentSubjectClaims should retrieve UUIDs from subject and roles`() = runTest {
        val roles = listOf("stellio-admin", "test-role")

        subjectReferentialService.getCurrentSubjectClaims()
            .shouldSucceedWith {
                assertEquals(5, it.size)
                assertThat(it).containsAll(roles.plus(USER_UUID).plus(GENERIC_SUBJECTS))
            }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `getGroups should return the group memberships for a user`() = runTest {
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
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            groupsMemberships = userGroupsUuids
        )

        subjectReferentialService.create(subjectReferential)

        val groups = subjectReferentialService.getGroups(0, 10)

        assertEquals(2, groups.size)
        groups.forEach {
            assertTrue(it.id in userGroupsUuids)
            assertTrue(it.name in userGroupsUuids)
            assertTrue(it.isMember)
        }

        subjectReferentialService.getCountGroups()
            .shouldSucceedWith { assertEquals(2, it) }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `getAllGroups should return all groups when caller has the admin role`() = runTest {
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
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            groupsMemberships = userGroupsUuids
        )

        subjectReferentialService.create(subjectReferential)

        val groups = subjectReferentialService.getAllGroups(0, 10)

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
    fun `getUsers should return all users`() = runTest {
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
    fun `getUsers should return a user with all available fields`() = runTest {
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
    fun `setGlobalRoles should update the global role of a subject`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            subjectType = SubjectType.USER
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.setGlobalRoles(USER_UUID, listOf(STELLIO_ADMIN))
        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                assertEquals(listOf(STELLIO_ADMIN), it.globalRoles)
            }

        subjectReferentialService.resetGlobalRoles(USER_UUID)
        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                assertNull(it.globalRoles)
            }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User", roles = ["stellio-admin"])
    fun `currentSubjectIsAdmin should find if an user is a stellio admin`() = runTest {
        subjectReferentialService.currentSubjectIsAdmin()
            .shouldSucceedWith {
                assertTrue(it)
            }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `currentSubjectIsAdmin should find if an user is not a stellio admin`() = runTest {
        subjectReferentialService.currentSubjectIsAdmin()
            .shouldSucceedWith {
                assertFalse(it)
            }
    }

    @Test
    fun `addGroupMembershipToUser should add a group membership to an user`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )

        subjectReferentialService.create(userAccessRights)

        subjectReferentialService.addGroupMembershipToUser(USER_UUID, GROUP_UUID)
            .shouldSucceed()

        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                assertThat(it.groupsMemberships).containsAll(listOf(GROUP_UUID))
            }
    }

    @Test
    fun `addGroupMembershipToUser should add a group membership to an user inside an existing list`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )

        subjectReferentialService.create(userAccessRights)
        subjectReferentialService.addGroupMembershipToUser(USER_UUID, GROUP_UUID)

        val newGroupUuid = UUID.randomUUID().toString()

        subjectReferentialService.addGroupMembershipToUser(USER_UUID, newGroupUuid)
            .shouldSucceed()

        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                assertThat(it.groupsMemberships).containsAll(listOf(GROUP_UUID, newGroupUuid))
            }
    }

    @Test
    fun `removeGroupMembershipToUser should remove a group membership to an user`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )

        subjectReferentialService.create(userAccessRights)
        subjectReferentialService.addGroupMembershipToUser(USER_UUID, GROUP_UUID)

        subjectReferentialService.removeGroupMembershipToUser(USER_UUID, GROUP_UUID)
            .shouldSucceed()

        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                assertTrue(it.groupsMemberships?.isEmpty() ?: false)
            }
    }

    @Test
    fun `updateSubjectInfo should update an existing subject info for an user`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = getSubjectInfoForUser("stellio")
        )

        subjectReferentialService.create(subjectReferential).shouldSucceed()

        subjectReferentialService.updateSubjectInfo(USER_UUID, Pair("username", "stellio v2"))
            .shouldSucceed()

        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                val newValue = it.getSubjectInfoValue()
                assertTrue(newValue.containsKey("username"))
                assertEquals("stellio v2", newValue["username"])
            }
    }

    @Test
    fun `updateSubjectInfo should add a subject info for an user`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = getSubjectInfoForUser("stellio")
        )

        subjectReferentialService.create(subjectReferential).shouldSucceed()

        subjectReferentialService.updateSubjectInfo(USER_UUID, Pair("givenName", "Blaise"))
            .shouldSucceed()

        subjectReferentialService.retrieve(USER_UUID)
            .shouldSucceedWith {
                val newValue = it.getSubjectInfoValue()
                assertTrue(newValue.containsKey("username"))
                assertEquals("stellio", newValue["username"])
                assertTrue(newValue.containsKey("givenName"))
                assertEquals("Blaise", newValue["givenName"])
            }
    }

    @Test
    fun `delete should remove the subject referential from persistence`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(userAccessRights)

        subjectReferentialService.delete(USER_UUID)
            .shouldSucceed()

        subjectReferentialService.retrieve(USER_UUID)
            .fold({
                assertInstanceOf(AccessDeniedException::class.java, it)
            }, {
                fail("it should have returned a AccessDeniedException exception")
            })
    }

    @Test
    fun `delete should remove the referential for a service-account subject`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = SERVICE_ACCOUNT_UUID,
            subjectType = SubjectType.CLIENT,
            subjectInfo = getSubjectInfoForClient("client-id", "kc-id"),
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential)

        // when deleting a client, event will only contain internal KC id
        subjectReferentialService.delete("kc-id")
            .shouldSucceed()

        subjectReferentialService.retrieve(SERVICE_ACCOUNT_UUID)
            .fold({
                assertInstanceOf(AccessDeniedException::class.java, it)
            }, {
                fail("it should have returned a AccessDeniedException exception")
            })
    }

    @Test
    fun `hasOneOfGlobalRoles should return false when user does not have the requested roles`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD
        )
        subjectReferentialService.create(subjectReferential)
        subjectReferentialService.hasOneOfGlobalRoles(listOf(USER_UUID), CREATION_ROLES)
            .shouldSucceedWith { assertFalse(it) }
    }

    @Test
    fun `hasOneOfGlobalRoles should return true when user has admin roles`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_ADMIN)
        )
        subjectReferentialService.create(subjectReferential)
        subjectReferentialService.hasOneOfGlobalRoles(listOf(USER_UUID), ADMIN_ROLES)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `hasOneOfGlobalRoles should return true when user has creation roles`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = USER_UUID,
            subjectType = SubjectType.USER,
            subjectInfo = EMPTY_JSON_PAYLOAD,
            globalRoles = listOf(STELLIO_CREATOR, STELLIO_ADMIN)
        )
        subjectReferentialService.create(subjectReferential)
        subjectReferentialService.hasOneOfGlobalRoles(listOf(USER_UUID), CREATION_ROLES)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User", groupsUUIDs = [GROUP_UUID])
    fun `hasOneOfGlobalRoles should return true when user has a global role inherited from a group`() = runTest {
        subjectReferentialService.create(
            SubjectReferential(
                subjectId = USER_UUID,
                subjectType = SubjectType.USER,
                subjectInfo = EMPTY_JSON_PAYLOAD,
                globalRoles = emptyList(),
                groupsMemberships = listOf(GROUP_UUID)
            )
        )
        subjectReferentialService.create(
            SubjectReferential(
                subjectId = GROUP_UUID,
                subjectType = SubjectType.GROUP,
                subjectInfo = getSubjectInfoForGroup(GROUP_UUID),
                globalRoles = listOf(STELLIO_CREATOR, STELLIO_ADMIN)
            )
        )
        val uuids = subjectReferentialService.getCurrentSubjectClaims().shouldSucceedAndResult()
        subjectReferentialService.hasOneOfGlobalRoles(uuids, CREATION_ROLES)
            .shouldSucceedWith { assertTrue(it) }
    }
}
