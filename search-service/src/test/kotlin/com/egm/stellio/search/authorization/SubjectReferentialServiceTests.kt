package com.egm.stellio.search.authorization

import arrow.core.Some
import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.GlobalRole.STELLIO_ADMIN
import com.egm.stellio.shared.util.GlobalRole.STELLIO_CREATOR
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedWith
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
    fun `it should retrieve UUIDs from user and groups memberships`() = runTest {
        val groupsUuids = List(3) { UUID.randomUUID().toString() }
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
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
            subjectType = SubjectType.USER
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
    fun `it should update the global role of a subject`() = runTest {
        val subjectReferential = SubjectReferential(
            subjectId = subjectUuid,
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
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(subjectReferential)

        subjectReferentialService.hasStellioAdminRole(Some(subjectUuid))
            .shouldSucceedWith {
                assertTrue(it)
            }

        subjectReferentialService.resetGlobalRoles(subjectUuid)

        subjectReferentialService.hasStellioAdminRole(Some(subjectUuid))
            .shouldSucceedWith {
                assertFalse(it)
            }

        subjectReferentialService.setGlobalRoles(subjectUuid, listOf(STELLIO_ADMIN, STELLIO_CREATOR))

        subjectReferentialService.hasStellioAdminRole(Some(subjectUuid))
            .shouldSucceedWith {
                assertTrue(it)
            }
    }

    @Test
    fun `it should add a group membership to an user`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER
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
            subjectType = SubjectType.USER
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
            subjectType = SubjectType.USER
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
            subjectType = SubjectType.USER
        )

        subjectReferentialService.create(userAccessRights)

        val serviceAccountId = UUID.randomUUID().toString()

        subjectReferentialService.addServiceAccountIdToClient(subjectUuid, serviceAccountId)
            .shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .shouldSucceedWith {
                assertEquals(serviceAccountId, it.serviceAccountId)
            }
    }

    @Test
    fun `it should delete a subject referential`() = runTest {
        val userAccessRights = SubjectReferential(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRoles = listOf(STELLIO_ADMIN)
        )

        subjectReferentialService.create(userAccessRights)

        subjectReferentialService.delete(subjectUuid)
            .shouldSucceed()

        subjectReferentialService.retrieve(subjectUuid)
            .fold({
                assertInstanceOf(ResourceNotFoundException::class.java, it)
            }, {
                fail("it should have returned a ResourceNotFoundException exception")
            })
    }
}
