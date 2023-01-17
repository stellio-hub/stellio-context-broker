package com.egm.stellio.search.authorization

import arrow.core.*
import arrow.core.continuations.either
import arrow.fx.coroutines.parTraverseEither
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_CONTEXT
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
        userIsOneOfGivenRoles(ADMIN_ROLES, sub)

    override suspend fun userCanCreateEntities(sub: Option<Sub>): Either<APIException, Unit> =
        userIsOneOfGivenRoles(CREATION_ROLES, sub)

    private suspend fun userIsOneOfGivenRoles(roles: Set<GlobalRole>, sub: Option<Sub>): Either<APIException, Unit> {
        val matchingRoles = subjectReferentialService.getGlobalRoles(sub)
            .flattenOption()
            .intersect(roles)

        return if (matchingRoles.isEmpty())
            AccessDeniedException("User does not have any of the required roles: $roles").left()
        else Unit.right()
    }

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

    override suspend fun userIsAdminOfEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
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

    override suspend fun createAdminLink(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        createAdminLinks(listOf(entityId), sub)

    override suspend fun createAdminLinks(entitiesId: List<URI>, sub: Option<Sub>): Either<APIException, Unit> =
        entitiesId.parTraverseEither {
            entityAccessRightsService.setAdminRoleOnEntity((sub as Some).value, it)
        }.map { it.first() }

    override suspend fun getAuthorizedEntities(
        queryParams: QueryParams,
        context: String,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>> = either {
        val accessRights = queryParams.attrs.mapNotNull { AccessRight.forExpandedAttributeName(it).orNull() }
        val entitiesAccessControl = entityAccessRightsService.getSubjectAccessRights(
            sub,
            accessRights,
            queryParams.types,
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
            .map { it.serializeProperties() }
            .map { JsonLdEntity(it, listOf(context)) }

        val count = entityAccessRightsService.getSubjectAccessRightsCount(
            sub,
            accessRights,
            queryParams.types,
        ).bind()

        Pair(count, entitiesAccessControlWithSubjectRights)
    }

    override suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>> {

        return either {
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
                    listOf(AUTHORIZATION_CONTEXT)
                )
            }

            Pair(groups.first, jsonLdEntities)
        }
    }

    override suspend fun computeAccessRightFilter(sub: Option<Sub>): () -> String? {
        if (subjectReferentialService.hasStellioAdminRole(sub).getOrElse { false })
            return { null }
        else {
            return subjectReferentialService.getSubjectAndGroupsUUID(sub)
                .map {
                    if (it.isEmpty()) {
                        { "(specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')" }
                    } else {
                        {
                            """
                        ( 
                            (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                            OR
                            (tea.entity_id IN (
                                SELECT entity_id
                                FROM entity_access_rights
                                WHERE subject_id IN (${it.toListOfString()})
                            ))
                        )
                            """.trimIndent()
                        }
                    }
                }.getOrElse { { null } }
        }
    }

    private fun <T> List<T>.toListOfString() = this.joinToString(",") { "'$it'" }
}
