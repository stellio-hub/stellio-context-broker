package com.egm.stellio.search.authorization

import arrow.core.*
import arrow.core.raise.either
import arrow.fx.coroutines.parMap
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
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
            listOf(AccessRight.R_CAN_ADMIN, AccessRight.R_CAN_WRITE, AccessRight.R_CAN_READ),
            listOf(SpecificAccessPolicy.AUTH_WRITE, SpecificAccessPolicy.AUTH_READ),
            sub
        ).toAccessDecision(ENTITIY_READ_FORBIDDEN_MESSAGE)

    override suspend fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        userHasOneOfGivenRightsOnEntity(
            entityId,
            listOf(AccessRight.R_CAN_ADMIN, AccessRight.R_CAN_WRITE),
            listOf(SpecificAccessPolicy.AUTH_WRITE),
            sub
        ).toAccessDecision(ENTITY_UPDATE_FORBIDDEN_MESSAGE)

    override suspend fun userCanAdminEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        userHasOneOfGivenRightsOnEntity(
            entityId,
            listOf(AccessRight.R_CAN_ADMIN),
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

    override suspend fun createAdminRight(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        createAdminRights(listOf(entityId), sub)

    override suspend fun createAdminRights(entitiesId: List<URI>, sub: Option<Sub>): Either<APIException, Unit> =
        either {
            entitiesId.parMap {
                entityAccessRightsService.setAdminRoleOnEntity((sub as Some).value, it).bind()
            }
        }.map { it.first() }

    override suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit> =
        entityAccessRightsService.removeRolesOnEntity(entityId)

    override suspend fun getAuthorizedEntities(
        queryParams: QueryParams,
        contextLink: String,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>> = either {
        val accessRights = queryParams.attrs.mapNotNull { AccessRight.forExpandedAttributeName(it).getOrNull() }
        val entitiesAccessControl = entityAccessRightsService.getSubjectAccessRights(
            sub,
            accessRights,
            queryParams.type,
            queryParams.limit,
            queryParams.offset
        ).bind()

        // for each entity user is admin of, retrieve the full details of rights other users have on it

        val entitiesWithAdminRight = entitiesAccessControl.filter {
            it.right == AccessRight.R_CAN_ADMIN
        }.map { it.id }

        val rightsForEntities =
            entityAccessRightsService.getAccessRightsForEntities(sub, entitiesWithAdminRight).bind()

        val entitiesAccessControlWithSubjectRights = entitiesAccessControl
            .map { entityAccessControl ->
                if (rightsForEntities.containsKey(entityAccessControl.id)) {
                    val rightsForEntity = rightsForEntities[entityAccessControl.id]!!
                    entityAccessControl.copy(
                        rCanReadUsers = rightsForEntity[AccessRight.R_CAN_READ],
                        rCanWriteUsers = rightsForEntity[AccessRight.R_CAN_WRITE],
                        rCanAdminUsers = rightsForEntity[AccessRight.R_CAN_ADMIN]
                    )
                } else entityAccessControl
            }
            .map { it.serializeProperties(contextLink) }
            .map { JsonLdEntity(it, listOf(contextLink)) }

        val count = entityAccessRightsService.getSubjectAccessRightsCount(
            sub,
            accessRights,
            queryParams.type
        ).bind()

        Pair(count, entitiesAccessControlWithSubjectRights)
    }

    override suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contextLink: String,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>> = either {
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
            JsonLdEntity(
                it.serializeProperties(),
                listOf(contextLink)
            )
        }

        Pair(groups.first, jsonLdEntities)
    }

    override suspend fun getUsers(
        offset: Int,
        limit: Int,
        contextLink: String
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>> = either {
        val users = subjectReferentialService.getUsers(offset, limit)
        val usersCount = subjectReferentialService.getUsersCount().bind()

        val jsonLdEntities = users.map {
            JsonLdEntity(
                it.serializeProperties(contextLink),
                listOf(contextLink)
            )
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
