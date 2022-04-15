package com.egm.stellio.entity.authorization

import arrow.core.Option
import arrow.core.flattenOption
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.ADMIN_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.READ_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.AuthContextModel.USER_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.WRITE_RIGHTS
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled")
class Neo4jAuthorizationService(
    private val neo4jAuthorizationRepository: Neo4jAuthorizationRepository
) : AuthorizationService {

    override fun getSubjectUri(sub: Option<Sub>): URI =
        neo4jAuthorizationRepository.getSubjectUri((USER_PREFIX + sub.toStringValue()).toUri())

    override fun getSubjectGroups(sub: Option<Sub>): Set<URI> =
        neo4jAuthorizationRepository.getSubjectGroups(getSubjectUri(sub))

    override fun userIsAdmin(sub: Option<Sub>): Boolean = userIsOneOfGivenRoles(ADMIN_ROLES, sub)

    override fun userCanCreateEntities(sub: Option<Sub>): Boolean = userIsOneOfGivenRoles(CREATION_ROLES, sub)

    private fun userIsOneOfGivenRoles(roles: Set<GlobalRole>, sub: Option<Sub>): Boolean =
        neo4jAuthorizationRepository.getSubjectRoles(getSubjectUri(sub))
            .map { GlobalRole.forKey(it) }
            .flattenOption()
            .intersect(roles)
            .isNotEmpty()

    override fun filterEntitiesUserCanRead(entitiesId: List<URI>, sub: Option<Sub>): List<URI> {
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

    override fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, sub: Option<Sub>): List<URI> {
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

    override fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, sub: Option<Sub>): List<URI> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, ADMIN_RIGHTS, sub)

    override fun splitEntitiesByUserCanAdmin(
        entitiesId: List<URI>,
        sub: Option<Sub>
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
        sub: Option<Sub>
    ): List<URI> =
        if (userIsAdmin(sub))
            entitiesId
        else neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            getSubjectUri(sub),
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

    override fun userCanReadEntity(entityId: URI, sub: Option<Sub>): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, READ_RIGHTS, sub) ||
            entityHasSpecificAccessPolicy(
                entityId,
                listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ)
            )

    override fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, WRITE_RIGHTS, sub) ||
            entityHasSpecificAccessPolicy(entityId, listOf(SpecificAccessPolicy.AUTH_WRITE))

    override fun userIsAdminOfEntity(entityId: URI, sub: Option<Sub>): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, ADMIN_RIGHTS, sub)

    private fun userHasOneOfGivenRightsOnEntity(
        entityId: URI,
        rights: Set<String>,
        sub: Option<Sub>
    ): Boolean =
        userIsAdmin(sub) || neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            getSubjectUri(sub),
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

    override fun createAdminLink(entityId: URI, sub: Option<Sub>) {
        createAdminLinks(listOf(entityId), sub)
    }

    override fun createAdminLinks(entitiesId: List<URI>, sub: Option<Sub>) {
        val relationships = entitiesId.map {
            Relationship(
                objectId = it,
                type = listOf(AUTH_REL_CAN_ADMIN),
                datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
            )
        }
        neo4jAuthorizationRepository.createAdminLinks(
            getSubjectUri(sub),
            relationships,
            entitiesId
        )
    }

    override fun removeUserRightsOnEntity(entityId: URI, subjectId: URI) =
        neo4jAuthorizationRepository.removeUserRightsOnEntity(subjectId, entityId)

    override fun getAuthorizedEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        offset: Int,
        limit: Int,
        contextLink: String,
        includeSysAttrs: Boolean
    ): Pair<Int, List<JsonLdEntity>> {
        val userAndGroupIds = getSubjectGroups(sub)
            .plus(getSubjectUri(sub))
            .map { it.toString() }

        val result = if (userIsAdmin(sub))
            neo4jAuthorizationRepository.getAuthorizedEntitiesWithoutAuthentication(
                queryParams,
                offset,
                limit
            )
        else
            neo4jAuthorizationRepository.getAuthorizedEntitiesWithAuthentication(
                queryParams,
                offset,
                limit,
                userAndGroupIds
            )

        val jsonLdEntities = result.second.map {
            JsonLdEntity(
                it.serializeProperties(includeSysAttrs),
                it.contexts
            )
        }

        return Pair(result.first, jsonLdEntities)
    }
}
