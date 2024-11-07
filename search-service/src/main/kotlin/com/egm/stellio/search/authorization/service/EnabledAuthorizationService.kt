package com.egm.stellio.search.authorization.service

import arrow.core.Either
import arrow.core.Option
import arrow.core.Some
import arrow.core.flatMap
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.ADMIN_ROLES
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.CREATION_ROLES
import com.egm.stellio.shared.util.ENTITIY_READ_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.ENTITY_ADMIN_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.ENTITY_UPDATE_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.Sub
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled")
class EnabledAuthorizationService(
    private val subjectReferentialService: SubjectReferentialService,
    private val entityAccessRightsService: EntityAccessRightsService
) : AuthorizationService {

    override suspend fun userIsAdmin(sub: Option<Sub>): Either<APIException, Unit> =
        userHasOneOfGivenRoles(ADMIN_ROLES, sub)
            .toAccessDecision("User does not have any of the required roles: $ADMIN_ROLES")

    override suspend fun userCanCreateEntities(sub: Option<Sub>): Either<APIException, Unit> =
        userHasOneOfGivenRoles(CREATION_ROLES, sub)
            .toAccessDecision("User does not have any of the required roles: $CREATION_ROLES")

    internal suspend fun userHasOneOfGivenRoles(
        roles: Set<GlobalRole>,
        sub: Option<Sub>
    ): Either<APIException, Boolean> =
        subjectReferentialService.getSubjectAndGroupsUUID(sub)
            .flatMap { uuids -> subjectReferentialService.hasOneOfGlobalRoles(uuids, roles) }

    private fun Either<APIException, Boolean>.toAccessDecision(errorMessage: String) =
        this.flatMap {
            if (it)
                Unit.right()
            else
                AccessDeniedException(errorMessage).left()
        }

    override suspend fun userCanReadEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        userHasOneOfGivenRightsOnEntity(
            entityId,
            listOf(AccessRight.IS_OWNER, AccessRight.CAN_ADMIN, AccessRight.CAN_WRITE, AccessRight.CAN_READ),
            listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ),
            sub
        ).toAccessDecision(ENTITIY_READ_FORBIDDEN_MESSAGE)

    override suspend fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        userHasOneOfGivenRightsOnEntity(
            entityId,
            listOf(AccessRight.IS_OWNER, AccessRight.CAN_ADMIN, AccessRight.CAN_WRITE),
            listOf(SpecificAccessPolicy.AUTH_WRITE),
            sub
        ).toAccessDecision(ENTITY_UPDATE_FORBIDDEN_MESSAGE)

    override suspend fun userCanAdminEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        userHasOneOfGivenRightsOnEntity(
            entityId,
            listOf(AccessRight.IS_OWNER, AccessRight.CAN_ADMIN),
            emptyList(),
            sub
        ).toAccessDecision(ENTITY_ADMIN_FORBIDDEN_MESSAGE)

    private suspend fun userHasOneOfGivenRightsOnEntity(
        entityId: URI,
        rights: List<AccessRight>,
        specificAccessPolicies: List<SpecificAccessPolicy>,
        sub: Option<Sub>
    ): Either<APIException, Boolean> =
        entityAccessRightsService.checkHasRightOnEntity(
            sub,
            entityId,
            specificAccessPolicies,
            rights
        )

    override suspend fun createOwnerRight(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        createOwnerRights(listOf(entityId), sub)

    override suspend fun createOwnerRights(entitiesId: List<URI>, sub: Option<Sub>): Either<APIException, Unit> =
        either {
            entitiesId.parMap {
                entityAccessRightsService.setOwnerRoleOnEntity((sub as Some).value, it).bind()
            }
        }.map { it.first() }

    override suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit> =
        entityAccessRightsService.removeRolesOnEntity(entityId)

    override suspend fun getAuthorizedEntities(
        entitiesQuery: EntitiesQuery,
        contexts: List<String>,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = either {
        val accessRights = entitiesQuery.attrs.mapNotNull { AccessRight.forExpandedAttributeName(it).getOrNull() }
        val entitiesAccessRights = entityAccessRightsService.getSubjectAccessRights(
            sub,
            accessRights,
            entitiesQuery.typeSelection,
            entitiesQuery.ids,
            entitiesQuery.paginationQuery
        ).bind()

        // for each entity user is admin or creator of, retrieve the full details of rights other users have on it

        val entitiesWithAdminRight = entitiesAccessRights.filter {
            listOf(AccessRight.CAN_ADMIN, AccessRight.IS_OWNER).contains(it.right)
        }.map { it.id }

        val rightsForAdminEntities =
            entityAccessRightsService.getAccessRightsForEntities(sub, entitiesWithAdminRight).bind()

        val entitiesAccessControlWithSubjectRights = entitiesAccessRights
            .map { entityAccessRight ->
                if (rightsForAdminEntities.containsKey(entityAccessRight.id)) {
                    val rightsForEntity = rightsForAdminEntities[entityAccessRight.id]!!
                    entityAccessRight.copy(
                        canRead = rightsForEntity[AccessRight.CAN_READ],
                        canWrite = rightsForEntity[AccessRight.CAN_WRITE],
                        canAdmin = rightsForEntity[AccessRight.CAN_ADMIN],
                        owner = rightsForEntity[AccessRight.IS_OWNER]?.get(0)
                    )
                } else entityAccessRight
            }
            .map { it.serializeProperties(contexts) }
            .map { ExpandedEntity(it) }

        val count = entityAccessRightsService.getSubjectAccessRightsCount(
            sub,
            accessRights,
            entitiesQuery.typeSelection,
            entitiesQuery.ids
        ).bind()

        Pair(count, entitiesAccessControlWithSubjectRights)
    }

    override suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contexts: List<String>,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = either {
        val groups =
            when (userIsAdmin(sub)) {
                is Either.Left -> {
                    val groups = subjectReferentialService.getGroups(sub, offset, limit)
                    val groupsCount = subjectReferentialService.getCountGroups(sub).bind()
                    Pair(groupsCount, groups)
                }

                is Either.Right -> {
                    val groups = subjectReferentialService.getAllGroups(sub, offset, limit)
                    val groupsCount = subjectReferentialService.getCountAllGroups().bind()
                    Pair(groupsCount, groups)
                }
            }

        val jsonLdEntities = groups.second.map {
            ExpandedEntity(it.serializeProperties())
        }

        Pair(groups.first, jsonLdEntities)
    }

    override suspend fun getUsers(
        offset: Int,
        limit: Int,
        contexts: List<String>,
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = either {
        val users = subjectReferentialService.getUsers(offset, limit)
        val usersCount = subjectReferentialService.getUsersCount().bind()

        val jsonLdEntities = users.map {
            ExpandedEntity(it.serializeProperties(contexts))
        }

        Pair(usersCount, jsonLdEntities)
    }

    override suspend fun computeAccessRightFilter(sub: Option<Sub>): () -> String? =
        subjectReferentialService.getSubjectAndGroupsUUID(sub).map { uuids ->
            if (subjectReferentialService.hasStellioAdminRole(uuids).getOrElse { false }) {
                { null }
            } else {
                {
                    """
                    ( 
                        (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                        OR
                        (entity_payload.entity_id IN (
                            SELECT entity_id
                            FROM entity_access_rights
                            WHERE subject_id IN (${uuids.toListOfString()})
                        ))
                    )
                    """.trimIndent()
                }
            }
        }.getOrElse { { "1 = 0" } }

    private fun <T> List<T>.toListOfString() = this.joinToString(",") { "'$it'" }
}
