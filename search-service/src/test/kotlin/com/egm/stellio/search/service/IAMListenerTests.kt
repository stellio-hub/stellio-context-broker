package com.egm.stellio.search.service

import com.egm.stellio.search.model.SubjectAccessRights
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
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
        val subjectCreateEvent = loadSampleData("authorization/UserCreateEvent.json")

        iamListener.processMessage(subjectCreateEvent)

        verify {
            subjectAccessRightsService.create(
                match {
                    it.subjectId == "urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0".toUri() &&
                        it.subjectType == SubjectAccessRights.SubjectType.USER &&
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
        val subjectDeleteEvent = loadSampleData("authorization/UserDeleteEvent.json")

        iamListener.processMessage(subjectDeleteEvent)

        verify {
            subjectAccessRightsService.delete(
                match {
                    it == "urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0".toUri()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role for a group`() {
        val roleAppendEvent = loadSampleData("authorization/RealmRoleAppendEventOneRole.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            subjectAccessRightsService.addAdminGlobalRole(
                match {
                    it == "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role for a client`() {
        val roleAppendEvent = loadSampleData("authorization/RealmRoleAppendToClient.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            subjectAccessRightsService.addAdminGlobalRole(
                match {
                    it == "urn:ngsi-ld:Client:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event adding a stellio-admin role within two roles`() {
        val roleAppendEvent = loadSampleData("authorization/RealmRoleAppendEventTwoRoles.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            subjectAccessRightsService.addAdminGlobalRole(
                match {
                    it == "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should handle an append event removing a stellio-admin role for a group`() {
        val roleAppendEvent = loadSampleData("authorization/RealmRoleAppendEventNoRole.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            subjectAccessRightsService.removeAdminGlobalRole(
                match {
                    it == "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb".toUri()
                }
            )
        }
        confirmVerified()
    }
}
