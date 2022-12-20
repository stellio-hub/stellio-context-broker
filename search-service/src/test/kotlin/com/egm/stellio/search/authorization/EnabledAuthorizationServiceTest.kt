package com.egm.stellio.search.authorization

import arrow.core.Some
import arrow.core.right
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

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
        Assertions.assertEquals(
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
}
