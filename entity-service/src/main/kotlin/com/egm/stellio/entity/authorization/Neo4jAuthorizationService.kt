package com.egm.stellio.entity.authorization

import arrow.core.Option
import arrow.core.flattenOption
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.ADMIN_ROLES
import com.egm.stellio.shared.util.AuthContextModel.ADMIN_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.READ_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.AuthContextModel.USER_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.WRITE_RIGHTS
import com.egm.stellio.shared.util.CREATION_ROLES
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.toStringValue
import com.egm.stellio.shared.util.toUri
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI
import java.util.UUID

@Component
@ConditionalOnProperty("application.authentication.enabled")
class Neo4jAuthorizationService(
    private val neo4jAuthorizationRepository: Neo4jAuthorizationRepository
) : AuthorizationService {

    override fun userIsAdmin(sub: Option<UUID>): Boolean = userIsOneOfGivenRoles(ADMIN_ROLES, sub)

    override fun userCanCreateEntities(sub: Option<UUID>): Boolean = userIsOneOfGivenRoles(CREATION_ROLES, sub)

    private fun userIsOneOfGivenRoles(roles: Set<GlobalRole>, sub: Option<UUID>): Boolean =
        neo4jAuthorizationRepository.getUserRoles((USER_PREFIX + sub.toStringValue()).toUri())
            .map { GlobalRole.forKey(it) }
            .flattenOption()
            .intersect(roles)
            .isNotEmpty()

    override fun filterEntitiesUserCanRead(entitiesId: List<URI>, sub: Option<UUID>): List<URI> {
        val authorizedBySpecificPolicyEntities =
            filterEntitiesWithSpecificAccessPolicy(
                entitiesId,
                listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ)
            )
        // remove the already authorized entities from the list to avoid double-checking them
        val grantedEntities = filterEntitiesUserHaveOneOfGivenRights(
            entitiesId.minus(authorizedBySpecificPolicyEntities.toSet()),
            READ_RIGHTS,
            sub
        )
        return authorizedBySpecificPolicyEntities.plus(grantedEntities)
    }

    override fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, sub: Option<UUID>): List<URI> {
        val authorizedBySpecificPolicyEntities =
            filterEntitiesWithSpecificAccessPolicy(entitiesId, listOf(SpecificAccessPolicy.AUTH_WRITE))
        // remove the already authorized entities from the list to avoid double-checking them
        val grantedEntities = filterEntitiesUserHaveOneOfGivenRights(
            entitiesId.minus(authorizedBySpecificPolicyEntities.toSet()),
            WRITE_RIGHTS,
            sub
        )
        return authorizedBySpecificPolicyEntities.plus(grantedEntities)
    }

    override fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, sub: Option<UUID>): List<URI> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, ADMIN_RIGHTS, sub)

    override fun splitEntitiesByUserCanAdmin(
        entitiesId: List<URI>,
        sub: Option<UUID>
    ): Pair<List<URI>, List<URI>> {
        val entitiesUserCanAdminIds =
            filterEntitiesUserCanAdmin(entitiesId, sub)
        return entitiesId.partition {
            entitiesUserCanAdminIds.contains(it)
        }
    }

    private fun filterEntitiesUserHaveOneOfGivenRights(
        entitiesId: List<URI>,
        rights: Set<String>,
        sub: Option<UUID>
    ): List<URI> =
        if (userIsAdmin(sub))
            entitiesId
        else neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            (USER_PREFIX + sub.toStringValue()).toUri(),
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

    override fun userCanReadEntity(entityId: URI, sub: Option<UUID>): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, READ_RIGHTS, sub) ||
            entityHasSpecificAccessPolicy(
                entityId,
                listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ)
            )

    override fun userCanUpdateEntity(entityId: URI, sub: Option<UUID>): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, WRITE_RIGHTS, sub) ||
            entityHasSpecificAccessPolicy(entityId, listOf(SpecificAccessPolicy.AUTH_WRITE))

    override fun userIsAdminOfEntity(entityId: URI, sub: Option<UUID>): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, ADMIN_RIGHTS, sub)

    private fun userHasOneOfGivenRightsOnEntity(
        entityId: URI,
        rights: Set<String>,
        sub: Option<UUID>
    ): Boolean =
        userIsAdmin(sub) || neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            (USER_PREFIX + sub.toStringValue()).toUri(),
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

    override fun createAdminLink(entityId: URI, sub: Option<UUID>) {
        createAdminLinks(listOf(entityId), sub)
    }

    override fun createAdminLinks(entitiesId: List<URI>, sub: Option<UUID>) {
        val relationships = entitiesId.map {
            Relationship(
                objectId = it,
                type = listOf(AUTH_REL_CAN_ADMIN),
                datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
            )
        }
        neo4jAuthorizationRepository.createAdminLinks(
            (USER_PREFIX + sub.toStringValue()).toUri(),
            relationships,
            entitiesId
        )
    }

    override fun removeUserRightsOnEntity(entityId: URI, subjectId: URI) =
        neo4jAuthorizationRepository.removeUserRightsOnEntity(subjectId, entityId)
}
