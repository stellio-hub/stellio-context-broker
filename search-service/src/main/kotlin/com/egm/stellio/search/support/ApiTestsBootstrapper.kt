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

    @Value("\${application.apitests.userid}")
    val apiTestUserId: String? = null

    override fun run(vararg args: String?) {
        runBlocking {
            subjectReferentialService.retrieve(apiTestUserId!!)
                .tapLeft {
                    val subjectReferential = SubjectReferential(
                        subjectId = apiTestUserId!!,
                        subjectType = SubjectType.USER,
                        subjectInfo = """
                            {"type":"Property","value":{"username":"api-tests-user@stellio.io"}}
                        """.trimIndent(),
                        globalRoles = listOf(GlobalRole.STELLIO_CREATOR)
                    )
                    subjectReferentialService.create(subjectReferential)
                }
        }
    }
}
