package com.egm.stellio.search.authorization.permission.service

import arrow.core.Either
import arrow.core.Option
import arrow.core.Some
import arrow.core.flatMap
import arrow.core.getOrElse
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
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.toStringValue
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled")
class EnabledAuthorizationService(
    private val subjectReferentialService: SubjectReferentialService,
    private val permissionService: PermissionService
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
        userCanDoActionOnEntity(
            entityId,
            Action.READ,
            sub
        ).toAccessDecision(ENTITIY_READ_FORBIDDEN_MESSAGE)

    override suspend fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        userCanDoActionOnEntity(
            entityId,
            Action.WRITE,
            sub
        ).toAccessDecision(ENTITY_UPDATE_FORBIDDEN_MESSAGE)

    override suspend fun userCanAdminEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        userCanDoActionOnEntity(
            entityId,
            Action.ADMIN,
            sub
        ).toAccessDecision(ENTITY_ADMIN_FORBIDDEN_MESSAGE)

    private suspend fun userCanDoActionOnEntity(
        entityId: URI,
        action: Action,
        sub: Option<Sub>
    ): Either<APIException, Boolean> =
        permissionService.checkHasPermissionOnEntity(
            sub,
            entityId,
            action
        )

    override suspend fun createOwnerRight(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        createOwnerRights(listOf(entityId), sub)

    override suspend fun createOwnerRights(entitiesId: List<URI>, sub: Option<Sub>): Either<APIException, Unit> =
        either {
            val subValue = (sub as Some).value
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
        sub: Option<Sub>
    ): Either<APIException, Unit> = permissionService.create(
        Permission(
            assignee = null,
            assigner = sub.toStringValue(),
            target = TargetAsset(id = entityId),
            action = action
        )
    )

    override suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit> =
        permissionService.removePermissionsOnEntity(entityId)

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
                        (entity_payload.entity_id IN (
                            SELECT target_id
                            FROM permission
                            WHERE assignee is null
                            OR assignee IN (${uuids.toListOfString()})
                        ))
                    """.trimIndent()
                }
            }
        }.getOrElse { { "1 = 0" } }

    private fun <T> List<T>.toListOfString() = this.joinToString(",") { "'$it'" }
}
