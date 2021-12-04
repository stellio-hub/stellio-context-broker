package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.CREATION_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_PREFIX
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.WRITE_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.SpecificAccessPolicy
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

    override fun userIsAdmin(userSub: String): Boolean = userIsOneOfGivenRoles(ADMIN_ROLES, userSub)

    override fun userCanCreateEntities(userSub: String): Boolean = userIsOneOfGivenRoles(CREATION_ROLES, userSub)

    private fun userIsOneOfGivenRoles(roles: Set<String>, userSub: String): Boolean =
        neo4jAuthorizationRepository.getUserRoles((USER_PREFIX + userSub).toUri())
            .intersect(roles)
            .isNotEmpty()

    override fun filterEntitiesUserCanRead(entitiesId: List<URI>, userSub: String): List<URI> {
        val authorizedBySpecificPolicyEntities =
            filterEntitiesWithSpecificAccessPolicy(
                entitiesId,
                listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ)
            )
        // remove the already authorized entities from the list to avoid double-checking them
        val grantedEntities = filterEntitiesUserHaveOneOfGivenRights(
            entitiesId.minus(authorizedBySpecificPolicyEntities.toSet()),
            READ_RIGHT,
            userSub
        )
        return authorizedBySpecificPolicyEntities.plus(grantedEntities)
    }

    override fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, userSub: String): List<URI> {
        val authorizedBySpecificPolicyEntities =
            filterEntitiesWithSpecificAccessPolicy(entitiesId, listOf(SpecificAccessPolicy.AUTH_WRITE))
        // remove the already authorized entities from the list to avoid double-checking them
        val grantedEntities = filterEntitiesUserHaveOneOfGivenRights(
            entitiesId.minus(authorizedBySpecificPolicyEntities.toSet()),
            WRITE_RIGHT,
            userSub
        )
        return authorizedBySpecificPolicyEntities.plus(grantedEntities)
    }

    override fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, userSub: String): List<URI> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, ADMIN_RIGHT, userSub)

    override fun splitEntitiesByUserCanAdmin(
        entitiesId: List<URI>,
        userSub: String
    ): Pair<List<URI>, List<URI>> {
        val entitiesUserCanAdminIds =
            filterEntitiesUserCanAdmin(entitiesId, userSub)
        return entitiesId.partition {
            entitiesUserCanAdminIds.contains(it)
        }
    }

    private fun filterEntitiesUserHaveOneOfGivenRights(
        entitiesId: List<URI>,
        rights: Set<String>,
        userSub: String
    ): List<URI> =
        if (userIsAdmin(userSub))
            entitiesId
        else neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            (USER_PREFIX + userSub).toUri(),
            entitiesId,
            rights
        )

    private fun filterEntitiesWithSpecificAccessPolicy(
        entitiesId: List<URI>,
        specificAccessPolicies: List<SpecificAccessPolicy>
    ): List<URI> =
        neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
            entitiesId,
            specificAccessPolicies.map { it.name }
        )

    override fun userCanReadEntity(entityId: URI, userSub: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, READ_RIGHT, userSub) ||
            entityHasSpecificAccessPolicy(
                entityId,
                listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ)
            )

    override fun userCanUpdateEntity(entityId: URI, userSub: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, WRITE_RIGHT, userSub) ||
            entityHasSpecificAccessPolicy(entityId, listOf(SpecificAccessPolicy.AUTH_WRITE))

    override fun userIsAdminOfEntity(entityId: URI, userSub: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, ADMIN_RIGHT, userSub)

    private fun userHasOneOfGivenRightsOnEntity(
        entityId: URI,
        rights: Set<String>,
        userSub: String
    ): Boolean =
        userIsAdmin(userSub) || neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            (USER_PREFIX + userSub).toUri(),
            listOf(entityId),
            rights
        ).isNotEmpty()

    private fun entityHasSpecificAccessPolicy(
        entityId: URI,
        specificAccessPolicies: List<SpecificAccessPolicy>
    ): Boolean =
        neo4jAuthorizationRepository
            .filterEntitiesWithSpecificAccessPolicy(
                listOf(entityId),
                specificAccessPolicies.map { it.name }
            )
            .isNotEmpty()

    override fun createAdminLink(entityId: URI, userSub: String) {
        createAdminLinks(listOf(entityId), userSub)
    }

    override fun createAdminLinks(entitiesId: List<URI>, userSub: String) {
        val relationships = entitiesId.map {
            Relationship(
                objectId = it,
                type = listOf(R_CAN_ADMIN),
                datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
            )
        }
        neo4jAuthorizationRepository.createAdminLinks(
            (USER_PREFIX + userSub).toUri(),
            relationships,
            entitiesId
        )
    }

    override fun removeUserRightsOnEntity(entityId: URI, subjectId: URI) =
        neo4jAuthorizationRepository.removeUserRightsOnEntity(subjectId, entityId)
}
