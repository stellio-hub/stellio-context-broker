package com.egm.stellio.entity.authorization

interface AuthorizationService {

    companion object {
        const val USER_PREFIX: String = "urn:ngsi-ld:User:"
        const val AUTHORIZATION_ONTOLOGY = "https://ontology.eglobalmark.com/authorization#"
        const val EGM_ROLES = AUTHORIZATION_ONTOLOGY + "roles"
        const val R_CAN_READ = AUTHORIZATION_ONTOLOGY + "rCanRead"
        const val R_CAN_WRITE = AUTHORIZATION_ONTOLOGY + "rCanWrite"
        const val R_CAN_ADMIN = AUTHORIZATION_ONTOLOGY + "rCanAdmin"
        const val SERVICE_ACCOUNT_ID = AUTHORIZATION_ONTOLOGY + "serviceAccountId"
        const val ADMIN_ROLE_LABEL = "stellio-admin"
        const val CREATION_ROLE_LABEL = "stellio-creator"
        val ADMIN_ROLES: Set<String> = setOf(ADMIN_ROLE_LABEL)
        val CREATION_ROLES: Set<String> = setOf(CREATION_ROLE_LABEL).plus(ADMIN_ROLES)
        val ADMIN_RIGHT: Set<String> = setOf(R_CAN_ADMIN)
        val WRITE_RIGHT: Set<String> = setOf(R_CAN_WRITE).plus(ADMIN_RIGHT)
        val READ_RIGHT: Set<String> = setOf(R_CAN_READ).plus(WRITE_RIGHT)
    }

    fun userIsAdmin(userId: String): Boolean
    fun userCanCreateEntities(userId: String): Boolean
    fun filterEntitiesUserCanRead(entitiesId: List<String>, userId: String): List<String>
    fun filterEntitiesUserCanUpdate(entitiesId: List<String>, userId: String): List<String>
    fun filterEntitiesUserCanAdmin(entitiesId: List<String>, userId: String): List<String>
    fun splitEntitiesByUserCanAdmin(entitiesId: List<String>, userId: String): Pair<List<String>, List<String>>
    fun userCanReadEntity(entityId: String, userId: String): Boolean
    fun userCanUpdateEntity(entityId: String, userId: String): Boolean
    fun userIsAdminOfEntity(entityId: String, userId: String): Boolean
    fun createAdminLink(entityId: String, userId: String)
    fun createAdminLinks(entitiesId: List<String>, userId: String)
}
