package com.egm.stellio.entity.util

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.repository.EntityRepository
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Profile("api-tests")
@Component
class ApiTestsBootstrapper(
    private val entityRepository: EntityRepository
) : CommandLineRunner {

    @Value("\${application.apitests.userid}")
    val apiTestUserId: String? = null

    companion object {
        val AUTHORIZATION_CONTEXTS: List<String> = listOf(
            "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/authorization/jsonld-contexts/authorization.jsonld",
            "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
        )
        const val USER_TYPE = "User"
        val USER_ROLES = listOf("stellio-creator")
    }

    override fun run(vararg args: String?) {
        // well, this should not happen in api-tests profile as we start from a fresh database on each run
        val apiTestsUser = entityRepository.findById(apiTestUserId!!)
        if (!apiTestsUser.isPresent) {
            val entity = Entity(
                id = apiTestUserId!!,
                type = listOf(USER_TYPE),
                contexts = AUTHORIZATION_CONTEXTS,
                properties = mutableListOf(
                    Property(
                        name = "roles",
                        value = USER_ROLES
                    ),
                    Property(
                        name = "username",
                        value = "API Tests"
                    )
                )
            )

            entityRepository.save(entity)
        }
    }
}
