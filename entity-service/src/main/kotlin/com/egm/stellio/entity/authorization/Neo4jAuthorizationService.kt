package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.CREATION_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_PREFIX
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.WRITE_RIGHT
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.toUri
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
        neo4jAuthorizationRepository.getUserRoles((USER_PREFIX + userId).toUri())
            .intersect(roles)
            .isNotEmpty()

    override fun filterEntitiesUserCanRead(entitiesId: List<URI>, userId: String): List<URI> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, READ_RIGHT, userId)

    override fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, userId: String): List<URI> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, WRITE_RIGHT, userId)

    override fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, userId: String): List<URI> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, ADMIN_RIGHT, userId)

    override fun splitEntitiesByUserCanAdmin(
        entitiesId: List<URI>,
        userId: String
    ):
        Pair<List<URI>, List<URI>> {
            val entitiesUserCanAdminIds =
                filterEntitiesUserCanAdmin(entitiesId, userId)
            return entitiesId.partition {
                entitiesUserCanAdminIds.contains(it)
            }
        }

    private fun filterEntitiesUserHaveOneOfGivenRights(
        entitiesId: List<URI>,
        rights: Set<String>,
        userId: String
    ): List<URI> =
        if (userIsAdmin(userId))
            entitiesId
        else neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            (USER_PREFIX + userId).toUri(),
            entitiesId,
            rights
        )

    override fun userCanReadEntity(entityId: URI, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, READ_RIGHT, userId)

    override fun userCanUpdateEntity(entityId: URI, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, WRITE_RIGHT, userId)

    override fun userIsAdminOfEntity(entityId: URI, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, ADMIN_RIGHT, userId)

    private fun userHasOneOfGivenRightsOnEntity(
        entityId: URI,
        rights: Set<String>,
        userId: String
    ): Boolean =
        userIsAdmin(userId) || neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            (USER_PREFIX + userId).toUri(),
            listOf(entityId),
            rights
        ).isNotEmpty()

    override fun createAdminLink(entityId: URI, userId: String) {
        createAdminLinks(listOf(entityId), userId)
    }

    override fun createAdminLinks(entitiesId: List<URI>, userId: String) {
        val relationships = entitiesId.map {
            Relationship(
                type = listOf(R_CAN_ADMIN),
                datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
            )
        }
        neo4jAuthorizationRepository.createAdminLinks(
            (USER_PREFIX + userId).toUri(),
            relationships,
            entitiesId
        )
    }
}
