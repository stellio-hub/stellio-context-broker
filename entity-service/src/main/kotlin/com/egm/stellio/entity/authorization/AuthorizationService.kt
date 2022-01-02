package com.egm.stellio.entity.authorization

import java.net.URI

interface AuthorizationService {
    fun userIsAdmin(userSub: String): Boolean
    fun userCanCreateEntities(userSub: String): Boolean
    fun filterEntitiesUserCanRead(entitiesId: List<URI>, userSub: String): List<URI>
    fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, userSub: String): List<URI>
    fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, userSub: String): List<URI>
    fun splitEntitiesByUserCanAdmin(entitiesId: List<URI>, userSub: String): Pair<List<URI>, List<URI>>
    fun userCanReadEntity(entityId: URI, userSub: String): Boolean
    fun userCanUpdateEntity(entityId: URI, userSub: String): Boolean
    fun userIsAdminOfEntity(entityId: URI, userSub: String): Boolean
    fun createAdminLink(entityId: URI, userSub: String)
    fun createAdminLinks(entitiesId: List<URI>, userSub: String)
    fun removeUserRightsOnEntity(entityId: URI, subjectId: URI): Int
}
