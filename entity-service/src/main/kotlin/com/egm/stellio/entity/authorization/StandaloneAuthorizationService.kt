package com.egm.stellio.entity.authorization

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class StandaloneAuthorizationService : AuthorizationService {
    override fun userIsAdmin(userId: String): Boolean {
        return true
    }

    override fun userCanCreateEntities(userId: String): Boolean {
        return true
    }

    override fun filterEntitiesUserCanRead(entitiesId: List<String>, userId: String): List<String> {
        return entitiesId
    }

    override fun filterEntitiesUserCanUpdate(entitiesId: List<String>, userId: String): List<String> {
        return entitiesId
    }

    override fun userCanReadEntity(entityId: String, userId: String): Boolean {
        return true
    }

    override fun userCanUpdateEntity(entityId: String, userId: String): Boolean {
        return true
    }

    override fun userIsAdminOfEntity(entityId: String, userId: String): Boolean {
        return true
    }

    override fun createAdminLink(entityId: String, userId: String) {}

    override fun createAdminLinks(entitiesId: List<String>, userId: String) {}
}
