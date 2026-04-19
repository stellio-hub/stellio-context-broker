package com.egm.stellio.search.authorization.permission.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.Scope
import com.egm.stellio.shared.util.ADMIN_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTHENTICATED_SUBJECT
import com.egm.stellio.shared.util.CREATION_ROLES
import com.egm.stellio.shared.util.ErrorMessages.Authorization.userNotAuthorizedToAdminEntityMessage
import com.egm.stellio.shared.util.ErrorMessages.Authorization.userNotAuthorizedToReadEntityMessage
import com.egm.stellio.shared.util.ErrorMessages.Authorization.userNotAuthorizedToUpdateEntityMessage
import com.egm.stellio.shared.util.ErrorMessages.Authorization.userNotHavingRequiredRolesMessage
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.getSubFromSecurityContext
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled")
class EnabledAuthorizationService(
    private val subjectReferentialService: SubjectReferentialService,
    private val permissionService: PermissionService
) : AuthorizationService {

    override suspend fun userIsAdmin(): Either<APIException, Unit> =
        userHasOneOfGivenRoles(ADMIN_ROLES)
            .toAccessDecision(userNotHavingRequiredRolesMessage(ADMIN_ROLES))

    override suspend fun userCanCreateEntities(): Either<APIException, Unit> =
        userHasOneOfGivenRoles(CREATION_ROLES)
            .toAccessDecision(userNotHavingRequiredRolesMessage(CREATION_ROLES))

    internal suspend fun userHasOneOfGivenRoles(
        roles: Set<GlobalRole>
    ): Either<APIException, Boolean> = either {
        subjectReferentialService.getCurrentSubjectClaims().bind().any { it in roles.map { it.key } }
    }

    private fun Either<APIException, Boolean>.toAccessDecision(errorMessage: String) =
        this.flatMap {
            if (it)
                Unit.right()
            else
                AccessDeniedException(errorMessage).left()
        }

    override suspend fun userCanReadEntity(entityId: URI): Either<APIException, Unit> =
        userCanDoActionOnEntity(
            entityId,
            Action.READ
        ).toAccessDecision(userNotAuthorizedToReadEntityMessage(entityId))

    override suspend fun userCanUpdateEntity(entityId: URI): Either<APIException, Unit> =
        userCanDoActionOnEntity(
            entityId,
            Action.WRITE,
        ).toAccessDecision(userNotAuthorizedToUpdateEntityMessage(entityId))

    override suspend fun userCanAdminEntity(entityId: URI): Either<APIException, Unit> =
        userCanDoActionOnEntity(
            entityId,
            Action.ADMIN,
        ).toAccessDecision(userNotAuthorizedToAdminEntityMessage(entityId))

    private suspend fun userCanDoActionOnEntity(
        entityId: URI,
        action: Action
    ): Either<APIException, Boolean> =
        permissionService.checkHasPermissionOnEntity(
            entityId,
            action
        )

    override suspend fun createEntityOwnerRight(entityId: URI): Either<APIException, Unit> =
        createEntitiesOwnerRights(listOf(entityId))

    override suspend fun createEntitiesOwnerRights(entitiesId: List<URI>): Either<APIException, Unit> =
        either {
            val subValue = getSubFromSecurityContext()
            entitiesId.parMap {
                permissionService.create(
                    Permission(
                        assignee = subValue,
                        assigner = subValue,
                        target = TargetAsset(id = it),
                        action = Action.OWN
                    )
                ).bind()
            }
        }.map { it.first() }

    override suspend fun createScopesOwnerRights(scopes: List<Scope>): Either<APIException, Unit> =
        either {
            if (scopes.isEmpty())
                return Unit.right()
            val subValue = getSubFromSecurityContext()
            permissionService.getNewScopesFromList(scopes).bind()
                .parMap {
                    permissionService.create(
                        Permission(
                            assignee = subValue,
                            assigner = subValue,
                            target = TargetAsset(scopes = listOf(it)),
                            action = Action.OWN
                        )
                    ).bind()
                }
        }

    override suspend fun createGlobalPermission(
        entityId: URI,
        action: Action,
    ): Either<APIException, Unit> = permissionService.create(
        Permission(
            assignee = AUTHENTICATED_SUBJECT,
            assigner = getSubFromSecurityContext(),
            target = TargetAsset(id = entityId),
            action = action
        )
    )

    override suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit> =
        permissionService.removePermissionsOnEntity(entityId)

    override suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = either {
        val groups =
            when (userIsAdmin()) {
                is Either.Left -> {
                    val groups = subjectReferentialService.getGroups(offset, limit)
                    val groupsCount = subjectReferentialService.getCountGroups().bind()
                    Pair(groupsCount, groups)
                }

                is Either.Right -> {
                    val groups = subjectReferentialService.getAllGroups(offset, limit)
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

    override suspend fun getAccessRightWithClauseAndFilter(action: Action): WithAndFilter? = either {
        val claims = subjectReferentialService.getCurrentSubjectClaims().bind()
        if (userIsAdmin().isRight())
            null
        else permissionService.buildCandidatePermissionsWithStatement(action, claims) to
            permissionService.buildAsRightOnEntityFilter(action, claims)
    }.fold({ "" to "false" }, { it })
}
