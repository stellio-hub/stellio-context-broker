package com.egm.stellio.search.authorization

import arrow.core.Either
import arrow.core.Some
import arrow.core.right
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.ZonedDateTime
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EnabledAuthorizationService::class])
@ActiveProfiles("test")
class EnabledAuthorizationServiceTest {

    @Autowired
    private lateinit var enabledAuthorizationService: EnabledAuthorizationService

    @MockkBean
    private lateinit var subjectReferentialService: SubjectReferentialService

    @MockkBean
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    private val subjectUuid = "0768A6D5-D87B-4209-9A22-8C40A8961A79"
    private val groupUuid = "220FC854-3609-404B-BC77-F2DFE332B27B"

    @Test
    fun `it should return a null filter is user has the stellio-admin role`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } returns true.right()

        val accessRightFilter = enabledAuthorizationService.computeAccessRightFilter(Some(subjectUuid))
        Assertions.assertNull(accessRightFilter())
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
                (tea.entity_id IN (
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
    fun `it should return serialized groups memberships along with a count`() = runTest {
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
    fun `it should returned serialied access control entities with a count`() = runTest {
        coEvery {
            entityAccessRightsService.getAccessRights(any(), any(), any(), any(), any())
        } returns listOf(
            EntityAccessControl(
                id = "urn:ngsi-ld:Beehive:01".toUri(),
                types = listOf(BEEHIVE_TYPE),
                right = AccessRight.R_CAN_WRITE,
                createdAt = ZonedDateTime.now()
            )
        ).right()
        coEvery { entityAccessRightsService.getCountAccessRights(any(), any(), any()) } returns Either.Right(1)

        enabledAuthorizationService.getAuthorizedEntities(
            QueryParams(
                types = setOf(BEEHIVE_TYPE),
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
    }
}
