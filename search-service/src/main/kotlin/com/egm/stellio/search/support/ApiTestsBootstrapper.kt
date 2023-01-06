package com.egm.stellio.search.support

import com.egm.stellio.search.authorization.SubjectReferential
import com.egm.stellio.search.authorization.SubjectReferentialService
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.SubjectType
import kotlinx.coroutines.runBlocking
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Profile("api-tests")
@Component
class ApiTestsBootstrapper(
    private val subjectReferentialService: SubjectReferentialService
) : CommandLineRunner {

    @Value("\${application.apitests.userid.1}")
    val apiTestUserId1: String? = null

    @Value("\${application.apitests.userid.2}")
    val apiTestUserId2: String? = null

    @Value("\${application.apitests.userid.3}")
    val apiTestUserId3: String? = null

    @Value("\${application.apitests.groupid.1}")
    val apiTestGroupId1: String? = null

    @Value("\${application.apitests.groupid.2}")
    val apiTestGroupId2: String? = null

    override fun run(vararg args: String?) {
        runBlocking {
            createSubject(
                apiTestUserId1!!,
                userSubject(subjectId = apiTestUserId1!!, groupMembership = apiTestGroupId1)
            )

            if (!apiTestUserId2.isNullOrEmpty()) {
                createSubject(
                    apiTestUserId2!!,
                    userSubject(subjectId = apiTestUserId2!!)
                )
            }

            if (!apiTestUserId3.isNullOrEmpty()) {
                createSubject(
                    apiTestUserId3!!,
                    userSubject(subjectId = apiTestUserId3!!, globalRoles = listOf(GlobalRole.STELLIO_ADMIN))
                )
            }

            if (!apiTestGroupId1.isNullOrEmpty()) {
                createSubject(
                    apiTestGroupId1!!,
                    groupSubject(apiTestGroupId1!!)
                )
            }

            if (!apiTestUserId2.isNullOrEmpty()) {
                createSubject(
                    apiTestGroupId2!!,
                    groupSubject(apiTestGroupId2!!)
                )
            }
        }
    }

    fun userSubject(
        subjectId: String,
        globalRoles: List<GlobalRole> = listOf(GlobalRole.STELLIO_CREATOR),
        groupMembership: String? = null
    ): SubjectReferential =
        SubjectReferential(
            subjectId = subjectId,
            subjectType = SubjectType.USER,
            subjectInfo = """
                {"type":"Property","value":{"username":"api-tests-user@stellio.io"}}
            """.trimIndent(),
            globalRoles = globalRoles,
            groupsMemberships =
            if (!groupMembership.isNullOrEmpty())
                listOf(groupMembership)
            else null
        )

    fun groupSubject(subjectId: String): SubjectReferential =
        SubjectReferential(
            subjectId = subjectId,
            subjectType = SubjectType.GROUP,
            subjectInfo = """
                {"type":"Property","value":{"name":"Group 1"}}
            """.trimIndent()
        )

    suspend fun createSubject(subjectId: String, subjectReferential: SubjectReferential) =
        subjectReferentialService.retrieve(subjectId)
            .tapLeft {
                subjectReferentialService.create(subjectReferential)
            }
}
