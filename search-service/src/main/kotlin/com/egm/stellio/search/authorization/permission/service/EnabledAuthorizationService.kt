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
import com.egm.stellio.shared.util.ADMIN_ROLES
import com.egm.stellio.shared.util.CREATION_ROLES
import com.egm.stellio.shared.util.ENTITIY_READ_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.ENTITY_ADMIN_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.ENTITY_UPDATE_FORBIDDEN_MESSAGE
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
            .toAccessDecision("User does not have any of the required roles: $ADMIN_ROLES")

    override suspend fun userCanCreateEntities(): Either<APIException, Unit> =
        userHasOneOfGivenRoles(CREATION_ROLES)
            .toAccessDecision("User does not have any of the required roles: $CREATION_ROLES")

    internal suspend fun userHasOneOfGivenRoles(
        roles: Set<GlobalRole>
    ): Either<APIException, Boolean> =
        subjectReferentialService.getSubjectAndGroupsUUID()
            .flatMap { uuids -> subjectReferentialService.hasOneOfGlobalRoles(uuids, roles) }

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
        ).toAccessDecision(ENTITIY_READ_FORBIDDEN_MESSAGE)

    override suspend fun userCanUpdateEntity(entityId: URI): Either<APIException, Unit> =
        userCanDoActionOnEntity(
            entityId,
            Action.WRITE,
        ).toAccessDecision(ENTITY_UPDATE_FORBIDDEN_MESSAGE)

    override suspend fun userCanAdminEntity(entityId: URI): Either<APIException, Unit> =
        userCanDoActionOnEntity(
            entityId,
            Action.ADMIN,
        ).toAccessDecision(ENTITY_ADMIN_FORBIDDEN_MESSAGE)

    private suspend fun userCanDoActionOnEntity(
        entityId: URI,
        action: Action
    ): Either<APIException, Boolean> =
        permissionService.checkHasPermissionOnEntity(
            entityId,
            action
        )

    override suspend fun createOwnerRight(entityId: URI): Either<APIException, Unit> =
        createOwnerRights(listOf(entityId))

    override suspend fun createOwnerRights(entitiesId: List<URI>): Either<APIException, Unit> =
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

    override suspend fun createGlobalPermission(
        entityId: URI,
        action: Action,
    ): Either<APIException, Unit> = permissionService.create(
        Permission(
            assignee = null,
            assigner = getSubFromSecurityContext().orEmpty(),
            target = TargetAsset(id = entityId),
            action = action
        )
    )

    override suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit> =
        permissionService.removePermissionsOnEntity(entityId)

    override suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contexts: List<String>
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

    override suspend fun getAccessRightFilter(): String? = either {
        val uuids = subjectReferentialService.getSubjectAndGroupsUUID().bind()
        if (subjectReferentialService.hasStellioAdminRole(uuids).bind())
            null
        else permissionService.buildAsRightOnEntityFilter(Action.READ, uuids)
    }.fold({ "false" }, { it })

    override suspend fun getAdminPermissionWithClause(): String? = either {
        val uuids = subjectReferentialService.getSubjectAndGroupsUUID().bind()
        if (subjectReferentialService.hasStellioAdminRole(uuids).bind())
            null
        else permissionService.buildAdminPermissionWithClause(Action.READ, uuids)
    }.fold({ "" }, { it })
}
