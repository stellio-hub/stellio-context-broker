package com.egm.stellio.entity.authorization

import arrow.core.Option
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI
import java.util.UUID

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class StandaloneAuthorizationService : AuthorizationService {
    override fun userIsAdmin(sub: Option<UUID>): Boolean {
        return true
    }

    override fun userCanCreateEntities(sub: Option<UUID>): Boolean {
        return true
    }

    override fun filterEntitiesUserCanRead(entitiesId: List<URI>, sub: Option<UUID>): List<URI> {
        return entitiesId
    }

    override fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, sub: Option<UUID>): List<URI> {
        return entitiesId
    }

    override fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, sub: Option<UUID>): List<URI> {
        return entitiesId
    }

    override fun splitEntitiesByUserCanAdmin(
        entitiesId: List<URI>,
        sub: Option<UUID>
    ): Pair<List<URI>, List<URI>> {
        return Pair(entitiesId, emptyList())
    }

    override fun userCanReadEntity(entityId: URI, sub: Option<UUID>): Boolean {
        return true
    }

    override fun userCanUpdateEntity(entityId: URI, sub: Option<UUID>): Boolean {
        return true
    }

    override fun userIsAdminOfEntity(entityId: URI, sub: Option<UUID>): Boolean {
        return true
    }

    override fun createAdminLink(entityId: URI, sub: Option<UUID>) {}

    override fun createAdminLinks(entitiesId: List<URI>, sub: Option<UUID>) {}

    override fun removeUserRightsOnEntity(entityId: URI, subjectId: URI): Int = 1
}
