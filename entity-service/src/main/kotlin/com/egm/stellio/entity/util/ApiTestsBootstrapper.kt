package com.egm.stellio.entity.util

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.AUTHZ_PROP_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.AUTHZ_PROP_USERNAME
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_LABEL
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_PREFIX
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.toUri
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

    override fun run(vararg args: String?) {
        // well, this should not happen in api-tests profile as we start from a fresh database on each run
        val ngsiLdUserId = (USER_PREFIX + apiTestUserId!!).toUri()
        val apiTestsUser = entityRepository.getEntityCoreById(ngsiLdUserId.toString())
        if (apiTestsUser == null) {
            val entity = Entity(
                id = ngsiLdUserId,
                type = listOf(USER_LABEL),
                contexts = listOf(
                    NGSILD_EGM_AUTHORIZATION_CONTEXT,
                    NGSILD_CORE_CONTEXT
                ),
                properties = mutableListOf(
                    Property(
                        name = AUTHZ_PROP_ROLES,
                        value = listOf(GlobalRole.STELLIO_CREATOR.key)
                    ),
                    Property(
                        name = AUTHZ_PROP_USERNAME,
                        value = "API Tests"
                    )
                )
            )

            entityRepository.save(entity)
        }
    }
}
