package com.egm.stellio.entity.authorization

interface AuthorizationService {

    companion object {
        const val USER_PREFIX: String = "urn:ngsi-ld:User:"
        const val AUTHORIZATION_ONTOLOGY = "https://ontology.eglobalmark.com/authorization#"
        const val R_CAN_READ = AUTHORIZATION_ONTOLOGY + "rCanRead"
        const val R_CAN_WRITE = AUTHORIZATION_ONTOLOGY + "rCanWrite"
        const val R_CAN_ADMIN = AUTHORIZATION_ONTOLOGY + "rCanAdmin"
        val ADMIN_ROLES: Set<String> = setOf("admin")
        val CREATION_ROLES: Set<String> = setOf("creator").plus(ADMIN_ROLES)
        val ADMIN_RIGHT: Set<String> = setOf(R_CAN_ADMIN)
        val WRITE_RIGHT: Set<String> = setOf(R_CAN_WRITE).plus(ADMIN_RIGHT)
        val READ_RIGHT: Set<String> = setOf(R_CAN_READ).plus(WRITE_RIGHT)
    }

    fun userIsAdmin(userId: String): Boolean
    fun userCanCreateEntities(userId: String): Boolean
    fun filterEntitiesUserCanRead(entitiesId: List<String>, userId: String): List<String>
    fun filterEntitiesUserCanWrite(entitiesId: List<String>, userId: String): List<String>
    fun userCanReadEntity(entityId: String, userId: String): Boolean
    fun userCanWriteEntity(entityId: String, userId: String): Boolean
    fun userCanAdminEntity(entityId: String, userId: String): Boolean
    fun createAdminLink(entityId: String, userId: String)
}
