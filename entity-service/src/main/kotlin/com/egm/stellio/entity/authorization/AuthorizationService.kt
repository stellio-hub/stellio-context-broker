package com.egm.stellio.entity.authorization

import arrow.core.Option
import java.net.URI
import java.util.UUID

interface AuthorizationService {
    fun userIsAdmin(sub: Option<UUID>): Boolean
    fun userCanCreateEntities(sub: Option<UUID>): Boolean
    fun filterEntitiesUserCanRead(entitiesId: List<URI>, sub: Option<UUID>): List<URI>
    fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, sub: Option<UUID>): List<URI>
    fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, sub: Option<UUID>): List<URI>
    fun splitEntitiesByUserCanAdmin(entitiesId: List<URI>, sub: Option<UUID>): Pair<List<URI>, List<URI>>
    fun userCanReadEntity(entityId: URI, sub: Option<UUID>): Boolean
    fun userCanUpdateEntity(entityId: URI, sub: Option<UUID>): Boolean
    fun userIsAdminOfEntity(entityId: URI, sub: Option<UUID>): Boolean
    fun createAdminLink(entityId: URI, sub: Option<UUID>)
    fun createAdminLinks(entitiesId: List<URI>, sub: Option<UUID>)
    fun removeUserRightsOnEntity(entityId: URI, subjectId: URI): Int
}
