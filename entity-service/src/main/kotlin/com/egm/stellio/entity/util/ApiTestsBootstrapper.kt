package com.egm.stellio.entity.util

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.AUTHORIZATION_ONTOLOGY
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.CREATION_ROLE_LABEL
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_PREFIX
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.shared.util.JsonLdUtils.EGM_BASE_CONTEXT_URL
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.net.URI

@Profile("api-tests")
@Component
class ApiTestsBootstrapper(
    private val entityRepository: EntityRepository
) : CommandLineRunner {

    @Value("\${application.apitests.userid}")
    val apiTestUserId: String? = null

    companion object {
        val AUTHORIZATION_CONTEXTS: List<String> = listOf(
            "$EGM_BASE_CONTEXT_URL/authorization/jsonld-contexts/authorization.jsonld",
            "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
        )
        const val USER_TYPE = "User"
        val USER_ROLES = listOf(CREATION_ROLE_LABEL)
    }

    override fun run(vararg args: String?) {
        // well, this should not happen in api-tests profile as we start from a fresh database on each run
        val ngsiLdUserId = URI.create(USER_PREFIX + apiTestUserId!!)
        val apiTestsUser = entityRepository.getEntityCoreById(ngsiLdUserId.toString())
        if (apiTestsUser != null) {
            val entity = Entity(
                id = ngsiLdUserId,
                type = listOf(AUTHORIZATION_ONTOLOGY + USER_TYPE),
                contexts = AUTHORIZATION_CONTEXTS,
                properties = mutableListOf(
                    Property(
                        name = AUTHORIZATION_ONTOLOGY + "roles",
                        value = USER_ROLES
                    ),
                    Property(
                        name = AUTHORIZATION_ONTOLOGY + "username",
                        value = "API Tests"
                    )
                )
            )

            entityRepository.save(entity)
        }
    }
}
