package com.egm.stellio.entity.authorization

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class StandaloneAuthorizationService : AuthorizationService {
    override fun userIsAdmin(userId: String): Boolean {
        return true
    }

    override fun userIsCreator(userId: String): Boolean {
        return true
    }

    override fun filterEntitiesUserHasReadRight(entitiesId: List<String>, userId: String): List<String> {
        return entitiesId
    }

    override fun filterEntitiesUserHasWriteRight(entitiesId: List<String>, userId: String): List<String> {
        return entitiesId
    }

    override fun userHasReadRightsOnEntity(entityId: String, userId: String): Boolean {
        return true
    }

    override fun userHasWriteRightsOnEntity(entityId: String, userId: String): Boolean {
        return true
    }

    override fun userHasAdminRightsOnEntity(entityId: String, userId: String): Boolean {
        return true
    }

    override fun createAdminLink(entityId: String, userId: String) {}
}
