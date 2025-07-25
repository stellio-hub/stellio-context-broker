package com.egm.stellio.search.authorization.listener

import arrow.core.right
import com.egm.stellio.search.authorization.service.EntityAccessRightsService
import com.egm.stellio.search.authorization.service.SubjectReferentialService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [IAMListener::class])
@ActiveProfiles("test")
class IAMListenerTests {

    @Autowired
    private lateinit var iamListener: IAMListener

    @MockkBean(relaxed = true)
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var searchProperties: SearchProperties

    @MockkBean(relaxed = true)
    private lateinit var subjectReferentialService: SubjectReferentialService

    private val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should handle a create event for an user`() = runTest {
        val subjectCreateEvent = loadSampleData("events/authorization/UserCreateEvent.json")

        coEvery { subjectReferentialService.create(any()) } returns Unit.right()

        iamListener.dispatchIamMessage(subjectCreateEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.create(
                match {
                    it.subjectId == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0" &&
                        it.subjectType == SubjectType.USER &&
                        it.subjectInfo.asString() ==
                        """
                        {"type":"Property","value":{"username":"stellio","givenName":"John","familyName":"Doe"}}
                        """.trimIndent() &&
                        it.globalRoles == null
                }
            )
        }
    }

    @Test
    fun `it should handle a create event with unknwon properties for an user`() = runTest {
        val subjectCreateEvent = loadSampleData("events/authorization/UserCreateEventUnknownProperties.json")

        coEvery { subjectReferentialService.create(any()) } returns Unit.right()

        iamListener.dispatchIamMessage(subjectCreateEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.create(
                match {
                    it.subjectId == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0" &&
                        it.subjectType == SubjectType.USER &&
                        it.subjectInfo.asString() ==
                        """
                        {"type":"Property","value":{"username":"stellio","givenName":"John","familyName":"Doe"}}
                        """.trimIndent() &&
                        it.globalRoles == null
                }
            )
        }
    }

    @Test
    fun `it should handle a create event with the default tenant for an user`() = runTest {
        val subjectCreateEvent = loadSampleData("events/authorization/UserCreateEventWithDefaultTenant.json")

        coEvery { subjectReferentialService.create(any()) } returns Unit.right()

        iamListener.dispatchIamMessage(subjectCreateEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.create(
                match {
                    it.subjectId == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0" &&
                        it.subjectType == SubjectType.USER &&
                        it.subjectInfo.asString() ==
                        """
                        {"type":"Property","value":{"username":"stellio","givenName":"John","familyName":"Doe"}}
                        """.trimIndent() &&
                        it.globalRoles == null
                }
            )
        }
    }

    @Test
    fun `it should handle a create event for a group`() = runTest {
        val subjectCreateEvent = loadSampleData("events/authorization/GroupCreateEvent.json")

        coEvery { subjectReferentialService.create(any()) } returns Unit.right()

        iamListener.dispatchIamMessage(subjectCreateEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.create(
                match {
                    it.subjectId == "ab67edf3-238c-4f50-83f4-617c620c62eb" &&
                        it.subjectType == SubjectType.GROUP &&
                        it.subjectInfo.asString() ==
                        """
                        {"type":"Property","value":{"name":"EGM"}}
                        """.trimIndent() &&
                        it.globalRoles == null
                }
            )
        }
    }

    @Test
    fun `it should handle a create event for a subject with a default role`() = runTest {
        val subjectCreateEvent = loadSampleData("events/authorization/UserCreateEventDefaultRole.json")

        coEvery { subjectReferentialService.create(any()) } returns Unit.right()

        iamListener.dispatchIamMessage(subjectCreateEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.create(
                match {
                    it.subjectId == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0" &&
                        it.subjectType == SubjectType.USER &&
                        it.globalRoles == listOf(GlobalRole.STELLIO_CREATOR)
                }
            )
        }
    }

    @Test
    fun `it should handle a delete event for a subject`() = runTest {
        val subjectDeleteEvent = loadSampleData("events/authorization/UserDeleteEvent.json")

        coEvery { subjectReferentialService.delete(any()) } returns Unit.right()

        iamListener.dispatchIamMessage(subjectDeleteEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.delete(
                match {
                    it == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0"
                }
            )
        }
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role for a group`() = runTest {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventOneRole.json")

        coEvery { subjectReferentialService.setGlobalRoles(any(), any()) } returns Unit.right()

        iamListener.dispatchIamMessage(roleAppendEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.setGlobalRoles(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb"
                },
                eq(listOf(GlobalRole.STELLIO_ADMIN))
            )
        }
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role for a client`() = runTest {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendToClient.json")

        coEvery { subjectReferentialService.setGlobalRoles(any(), any()) } returns Unit.right()

        iamListener.dispatchIamMessage(roleAppendEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.setGlobalRoles(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb"
                },
                eq(listOf(GlobalRole.STELLIO_ADMIN))
            )
        }
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role within two roles`() = runTest {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventTwoRoles.json")

        coEvery { subjectReferentialService.setGlobalRoles(any(), any()) } returns Unit.right()

        iamListener.dispatchIamMessage(roleAppendEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.setGlobalRoles(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb"
                },
                eq(listOf(GlobalRole.STELLIO_ADMIN, GlobalRole.STELLIO_CREATOR))
            )
        }
    }

    @Test
    fun `it should handle an append event removing a stellio-admin role for a group`() = runTest {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventNoRole.json")

        coEvery { subjectReferentialService.resetGlobalRoles(any()) } returns Unit.right()

        iamListener.dispatchIamMessage(roleAppendEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.resetGlobalRoles(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb"
                }
            )
        }
    }

    @Test
    fun `it should handle an append event adding an user to a group`() = runTest {
        val roleAppendEvent = loadSampleData("events/authorization/GroupMembershipAppendEvent.json")

        coEvery { subjectReferentialService.addGroupMembershipToUser(any(), any()) } returns Unit.right()

        iamListener.dispatchIamMessage(roleAppendEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.addGroupMembershipToUser(
                match {
                    it == "96e1f1e9-d798-48d7-820e-59f5a9a2abf5"
                },
                match {
                    it == "7cdad168-96ee-4649-b768-a060ac2ef435"
                }
            )
        }
    }

    @Test
    fun `it should handle a delete event removing an user from a group`() = runTest {
        val roleAppendEvent = loadSampleData("events/authorization/GroupMembershipDeleteEvent.json")

        coEvery { subjectReferentialService.removeGroupMembershipToUser(any(), any()) } returns Unit.right()

        iamListener.dispatchIamMessage(roleAppendEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.removeGroupMembershipToUser(
                match {
                    it == "96e1f1e9-d798-48d7-820e-59f5a9a2abf5"
                },
                match {
                    it == "7cdad168-96ee-4649-b768-a060ac2ef435"
                }
            )
        }
    }

    @Test
    fun `it should handle a replace event changing the name of a group`() = runTest {
        val roleAppendEvent = loadSampleData("events/authorization/GroupUpdateEvent.json")

        coEvery { subjectReferentialService.updateSubjectInfo(any(), any()) } returns Unit.right()

        iamListener.dispatchIamMessage(roleAppendEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.updateSubjectInfo(
                eq("ab67edf3-238c-4f50-83f4-617c620c62eb"),
                match {
                    it.first == "name" && it.second == "EGM Team"
                }
            )
        }
    }

    @Test
    fun `it should handle a replace event changing the given name of an user`() = runTest {
        val roleAppendEvent = loadSampleData("events/authorization/UserUpdateEvent.json")

        coEvery { subjectReferentialService.updateSubjectInfo(any(), any()) } returns Unit.right()

        iamListener.dispatchIamMessage(roleAppendEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.updateSubjectInfo(
                eq("6ad19fe0-fc11-4024-85f2-931c6fa6f7e0"),
                match {
                    it.first == "givenName" && it.second == "Jonathan"
                }
            )
        }
    }

    @Test
    fun `it should handle a replace event changing the clientId of a client`() = runTest {
        val clientIdReplaceEvent = loadSampleData("events/authorization/ClientIdAppendToClient.json")

        coEvery { subjectReferentialService.updateSubjectInfo(any(), any()) } returns Unit.right()

        iamListener.dispatchIamMessage(clientIdReplaceEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.updateSubjectInfo(
                eq("191a6f0d-df07-4697-afde-da9d8a91d954"),
                match {
                    it.first == "clientId" && it.second == "friendly-client-id"
                }
            )
        }
    }

    @Test
    fun `it should delete all entities owned by a user if user is deleted`() = runTest {
        val subjectDeleteEvent = loadSampleData("events/authorization/UserDeleteEvent.json")
        coEvery {
            entityAccessRightsService.setOwnerRoleOnEntity(
                "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0",
                entityId
            )
        } returns Unit.right()

        coEvery { subjectReferentialService.delete(any()) } returns Unit.right()
        every { searchProperties.onOwnerDeleteCascadeEntities } returns true
        coEvery {
            entityAccessRightsService.getEntitiesIdsOwnedBySubject("6ad19fe0-fc11-4024-85f2-931c6fa6f7e0")
        } returns listOf(entityId).right()
        coEvery {
            entityService.permanentlyDeleteEntity(entityId, true)
        } returns Unit.right()

        iamListener.dispatchIamMessage(subjectDeleteEvent)

        coVerify(timeout = 1000L) {
            subjectReferentialService.delete(
                match {
                    it == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0"
                }
            )
        }
        coVerify {
            entityService.permanentlyDeleteEntity(
                eq(entityId),
                eq(true)
            )
        }
    }
}
