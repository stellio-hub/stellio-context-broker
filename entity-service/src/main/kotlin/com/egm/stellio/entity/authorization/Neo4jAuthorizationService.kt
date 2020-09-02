package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.CREATION_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_PREFIX
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.WRITE_RIGHT
import com.egm.stellio.entity.model.Relationship
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled")
class Neo4jAuthorizationService(
    private val neo4jAuthorizationRepository: Neo4jAuthorizationRepository
) : AuthorizationService {

    override fun userIsAdmin(userId: String): Boolean = userIsOneOfGivenRoles(ADMIN_ROLES, userId)

    override fun userCanCreateEntities(userId: String): Boolean = userIsOneOfGivenRoles(CREATION_ROLES, userId)

    private fun userIsOneOfGivenRoles(roles: Set<String>, userId: String): Boolean =
        neo4jAuthorizationRepository.getUserRoles(USER_PREFIX + userId).intersect(roles).isNotEmpty()

    override fun filterEntitiesUserCanRead(entitiesId: List<String>, userId: String): List<String> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, READ_RIGHT, userId)

    override fun filterEntitiesUserCanUpdate(entitiesId: List<String>, userId: String): List<String> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, WRITE_RIGHT, userId)

    override fun filterEntitiesUserCanAdmin(entitiesId: List<String>, userId: String): List<String> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, ADMIN_RIGHT, userId)

    override fun splitEntitiesByUserCanAdmin(
        entitiesId: List<String>,
        userId: String
    ):
        Pair<List<String>, List<String>> {
            val entitiesUserCanAdminIds =
                filterEntitiesUserCanAdmin(entitiesId, userId)
            return entitiesId.partition {
                entitiesUserCanAdminIds.contains(it)
            }
        }

    private fun filterEntitiesUserHaveOneOfGivenRights(
        entitiesId: List<String>,
        rights: Set<String>,
        userId: String
    ): List<String> =
        if (userIsAdmin(userId))
            entitiesId
        else neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            USER_PREFIX + userId,
            entitiesId,
            rights
        )

    override fun userCanReadEntity(entityId: String, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, READ_RIGHT, userId)

    override fun userCanUpdateEntity(entityId: String, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, WRITE_RIGHT, userId)

    override fun userIsAdminOfEntity(entityId: String, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, ADMIN_RIGHT, userId)

    private fun userHasOneOfGivenRightsOnEntity(
        entityId: String,
        rights: Set<String>,
        userId: String
    ): Boolean =
        userIsAdmin(userId) || neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            USER_PREFIX + userId,
            listOf(entityId),
            rights
        ).isNotEmpty()

    override fun createAdminLink(entityId: String, userId: String) {
        createAdminLinks(listOf(entityId), userId)
    }

    override fun createAdminLinks(entitiesId: List<String>, userId: String) {
        val relationships = entitiesId.map {
            Relationship(
                type = listOf(R_CAN_ADMIN),
                datasetId = URI.create("urn:ngsi-ld:Dataset:rCanAdmin:$it")
            )
        }
        neo4jAuthorizationRepository.createAdminLinks(USER_PREFIX + userId, relationships, entitiesId)
    }
}
