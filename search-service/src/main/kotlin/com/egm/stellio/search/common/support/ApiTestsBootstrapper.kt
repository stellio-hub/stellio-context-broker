package com.egm.stellio.search.common.support

import com.egm.stellio.search.authorization.subject.model.SubjectReferential
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.SubjectType
import io.r2dbc.postgresql.codec.Json
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

    @Value("\${application.apitests.username.1}")
    val apiTestUsername1: String? = null

    @Value("\${application.apitests.userid.2}")
    val apiTestUserId2: String? = null

    @Value("\${application.apitests.username.2}")
    val apiTestUsername2: String? = null

    @Value("\${application.apitests.userid.3}")
    val apiTestUserId3: String? = null

    @Value("\${application.apitests.username.3}")
    val apiTestUsername3: String? = null

    @Value("\${application.apitests.groupid.1}")
    val apiTestGroupId1: String? = null

    @Value("\${application.apitests.group.name.1}")
    val apiTestGroupName1: String? = null

    @Value("\${application.apitests.groupid.2}")
    val apiTestGroupId2: String? = null

    @Value("\${application.apitests.group.name.2}")
    val apiTestGroupName2: String? = null

    override fun run(vararg args: String?) {
        runBlocking {
            createSubject(
                apiTestUserId1!!,
                userSubject(
                    subjectId = apiTestUserId1!!,
                    username = apiTestUsername1!!
                )
            )

            if (!apiTestUserId2.isNullOrEmpty()) {
                createSubject(
                    subjectId = apiTestUserId2!!,
                    userSubject(
                        subjectId = apiTestUserId2!!,
                        username = apiTestUsername2!!,
                        groupMembership = apiTestGroupId1
                    ),
                )
            }

            if (!apiTestUserId3.isNullOrEmpty()) {
                createSubject(
                    apiTestUserId3!!,
                    userSubject(
                        subjectId = apiTestUserId3!!,
                        username = apiTestUsername3!!,
                        globalRoles = listOf(GlobalRole.STELLIO_ADMIN)
                    )
                )
            }

            if (!apiTestGroupId1.isNullOrEmpty()) {
                createSubject(
                    apiTestGroupId1!!,
                    groupSubject(apiTestGroupId1!!, apiTestGroupName1!!)
                )
            }

            if (!apiTestUserId2.isNullOrEmpty()) {
                createSubject(
                    apiTestGroupId2!!,
                    groupSubject(apiTestGroupId2!!, apiTestGroupName2!!)
                )
            }
        }
    }

    fun userSubject(
        subjectId: String,
        username: String,
        globalRoles: List<GlobalRole> = listOf(GlobalRole.STELLIO_CREATOR),
        groupMembership: String? = null
    ): SubjectReferential =
        SubjectReferential(
            subjectId = subjectId,
            subjectType = SubjectType.USER,
            subjectInfo = Json.of(
                """
                {"type":"Property","value":{"username":"$username"}}
                """.trimIndent()
            ),
            globalRoles = globalRoles,
            groupsMemberships =
            if (!groupMembership.isNullOrEmpty())
                listOf(groupMembership)
            else null
        )

    fun groupSubject(subjectId: String, groupName: String): SubjectReferential =
        SubjectReferential(
            subjectId = subjectId,
            subjectType = SubjectType.GROUP,
            subjectInfo = Json.of(
                """
                {"type":"Property","value":{"name":"$groupName"}}
                """.trimIndent()
            )
        )

    suspend fun createSubject(subjectId: String, subjectReferential: SubjectReferential) =
        subjectReferentialService.retrieve(subjectId)
            .onLeft {
                subjectReferentialService.create(subjectReferential)
            }
}
