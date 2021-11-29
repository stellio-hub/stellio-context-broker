package com.egm.stellio.search.service

import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.SubjectType
import com.egm.stellio.shared.web.toUUID
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
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
    private lateinit var subjectAccessRightsService: SubjectAccessRightsService

    @Test
    fun `it should handle a create event for a subject`() {
        val subjectCreateEvent = loadSampleData("events/authorization/UserCreateEvent.json")

        iamListener.processMessage(subjectCreateEvent)

        verify {
            subjectAccessRightsService.create(
                match {
                    it.subjectId == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0".toUUID() &&
                        it.subjectType == SubjectType.USER &&
                        it.globalRole == null &&
                        it.allowedReadEntities.isNullOrEmpty() &&
                        it.allowedWriteEntities.isNullOrEmpty()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle a delete event for a subject`() {
        val subjectDeleteEvent = loadSampleData("events/authorization/UserDeleteEvent.json")

        iamListener.processMessage(subjectDeleteEvent)

        verify {
            subjectAccessRightsService.delete(
                match {
                    it == "6ad19fe0-fc11-4024-85f2-931c6fa6f7e0".toUUID()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role for a group`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventOneRole.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            subjectAccessRightsService.addAdminGlobalRole(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb".toUUID()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role for a client`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendToClient.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            subjectAccessRightsService.addAdminGlobalRole(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb".toUUID()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role within two roles`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventTwoRoles.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            subjectAccessRightsService.addAdminGlobalRole(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb".toUUID()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event removing a stellio-admin role for a group`() {
        val roleAppendEvent = loadSampleData("events/authorization/RealmRoleAppendEventNoRole.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            subjectAccessRightsService.removeAdminGlobalRole(
                match {
                    it == "ab67edf3-238c-4f50-83f4-617c620c62eb".toUUID()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event adding a right on an entity`() {
        val rightAppendEvent = loadSampleData("events/authorization/RightAddOnEntity.json")

        iamListener.processIamRights(rightAppendEvent)

        verify {
            subjectAccessRightsService.addReadRoleOnEntity(
                eq("312b30b4-9279-4f7e-bdc5-ec56d699bb7d".toUUID()),
                eq("urn:ngsi-ld:Beekeeper:01".toUri())
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an delete event removing a right on an entity`() {
        val rightRemoveEvent = loadSampleData("events/authorization/RightRemoveOnEntity.json")

        iamListener.processIamRights(rightRemoveEvent)

        verify {
            subjectAccessRightsService.removeRoleOnEntity(
                eq("312b30b4-9279-4f7e-bdc5-ec56d699bb7d".toUUID()),
                eq("urn:ngsi-ld:Beekeeper:01".toUri())
            )
        }
        confirmVerified()
    }
}
