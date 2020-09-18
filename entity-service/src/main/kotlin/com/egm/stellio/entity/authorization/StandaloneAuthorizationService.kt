package com.egm.stellio.entity.authorization

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class StandaloneAuthorizationService : AuthorizationService {
    override fun userIsAdmin(userId: String): Boolean {
        return true
    }

    override fun userCanCreateEntities(userId: String): Boolean {
        return true
    }

    override fun filterEntitiesUserCanRead(entitiesId: List<URI>, userId: String): List<URI> {
        return entitiesId
    }

    override fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, userId: String): List<URI> {
        return entitiesId
    }

    override fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, userId: String): List<URI> {
        return entitiesId
    }

    override fun splitEntitiesByUserCanAdmin(
        entitiesId: List<URI>,
        userId: String
    ): Pair<List<URI>, List<URI>> {
        return Pair(entitiesId, emptyList())
    }

    override fun userCanReadEntity(entityId: URI, userId: String): Boolean {
        return true
    }

    override fun userCanUpdateEntity(entityId: URI, userId: String): Boolean {
        return true
    }

    override fun userIsAdminOfEntity(entityId: URI, userId: String): Boolean {
        return true
    }

    override fun createAdminLink(entityId: URI, userId: String) {}

    override fun createAdminLinks(entitiesId: List<URI>, userId: String) {}
}
