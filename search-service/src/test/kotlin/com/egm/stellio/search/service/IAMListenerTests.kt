package com.egm.stellio.search.service

import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.verify
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
    private lateinit var subjectReferentialService: SubjectReferentialService

    @MockkBean(relaxed = true)
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @Test
    fun `it should handle a create event for a subject`() {
        val subjectCreateEvent = loadSampleData("events/authorization/UserCreateEvent.json")

        iamListener.processIam(subjectCreateEvent)

        verify {
            subjectReferentialService.create(
                match {
                    it.subjectId == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0" &&
                        it.subjectType == SubjectType.USER &&
                        it.globalRoles == null
                }
            )
        }
    }

    @Test
    fun `it should handle a create event for a subject with a default role`() {
        val subjectCreateEvent = loadSampleData("events/authorization/UserCreateEventDefaultRole.json")

        iamListener.processIam(subjectCreateEvent)

        verify {
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
    fun `it should handle a delete event for a subject`() {
        val subjectDeleteEvent = loadSampleData("events/authorization/UserDeleteEvent.json")

        iamListener.processIam(subjectDeleteEvent)

        verify {
            subjectReferentialService.delete(
                match {
                    it == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0"
                }
            )
        }
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role for a group`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventOneRole.json")

        iamListener.processIam(roleAppendEvent)

        verify {
            subjectReferentialService.setGlobalRoles(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb"
                },
                eq(listOf(GlobalRole.STELLIO_ADMIN))
            )
        }
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role for a client`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendToClient.json")

        iamListener.processIam(roleAppendEvent)

        verify {
            subjectReferentialService.setGlobalRoles(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb"
                },
                eq(listOf(GlobalRole.STELLIO_ADMIN))
            )
        }
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role within two roles`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventTwoRoles.json")

        iamListener.processIam(roleAppendEvent)

        verify {
            subjectReferentialService.setGlobalRoles(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb"
                },
                eq(listOf(GlobalRole.STELLIO_ADMIN, GlobalRole.STELLIO_CREATOR))
            )
        }
    }

    @Test
    fun `it should handle an append event removing a stellio-admin role for a group`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventNoRole.json")

        iamListener.processIam(roleAppendEvent)

        verify {
            subjectReferentialService.resetGlobalRoles(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb"
                }
            )
        }
    }

    @Test
    fun `it should handle an append event adding an user to a group`() {
        val roleAppendEvent = loadSampleData("events/authorization/GroupMembershipAppendEvent.json")

        iamListener.processIam(roleAppendEvent)

        verify {
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
    fun `it should handle a delete event removing an user from a group`() {
        val roleAppendEvent = loadSampleData("events/authorization/GroupMembershipDeleteEvent.json")

        iamListener.processIam(roleAppendEvent)

        verify {
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
    fun `it should handle an append event adding a service account id to a client`() {
        val roleAppendEvent = loadSampleData("events/authorization/ServiceAccountIdAppendEvent.json")

        iamListener.processIam(roleAppendEvent)

        verify {
            subjectReferentialService.addServiceAccountIdToClient(
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
    fun `it should handle an append event adding a right on an entity`() {
        val rightAppendEvent = loadSampleData("events/authorization/RightAddOnEntity.json")

        iamListener.processIamRights(rightAppendEvent)

        verify {
            entityAccessRightsService.setRoleOnEntity(
                eq("312b30b4-9279-4f7e-bdc5-ec56d699bb7d"),
                eq("urn:ngsi-ld:Beekeeper:01".toUri()),
                eq(AccessRight.R_CAN_READ)
            )
        }
    }

    @Test
    fun `it should handle an append event adding a specific access policy on an entity`() {
        val rightAppendEvent = loadSampleData("events/authorization/SpecificAccessPolicyAddOnEntity.json")

        iamListener.processIamRights(rightAppendEvent)

        verify {
            temporalEntityAttributeService.updateSpecificAccessPolicy(
                eq("urn:ngsi-ld:Beekeeper:01".toUri()),
                eq(AuthContextModel.SpecificAccessPolicy.AUTH_READ)
            )
        }
    }

    @Test
    fun `it should handle a delete event removing a right on an entity`() {
        val rightRemoveEvent = loadSampleData("events/authorization/RightRemoveOnEntity.json")

        iamListener.processIamRights(rightRemoveEvent)

        verify {
            entityAccessRightsService.removeRoleOnEntity(
                eq("312b30b4-9279-4f7e-bdc5-ec56d699bb7d"),
                eq("urn:ngsi-ld:Beekeeper:01".toUri())
            )
        }
    }

    @Test
    fun `it should handle a delete event removing a specific access policy on an entity`() {
        val rightRemoveEvent = loadSampleData("events/authorization/SpecificAccessPolicyRemoveOnEntity.json")

        iamListener.processIamRights(rightRemoveEvent)

        verify {
            temporalEntityAttributeService.removeSpecificAccessPolicy(
                eq("urn:ngsi-ld:Beekeeper:01".toUri())
            )
        }
    }
}
