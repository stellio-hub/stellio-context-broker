package com.egm.stellio.search.authorization.service

import arrow.core.Either
import arrow.core.right
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedEntity
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class DisabledAuthorizationService : AuthorizationService {
    override suspend fun userIsAdmin(): Either<APIException, Unit> = Unit.right()

    override suspend fun userCanCreateEntities(): Either<APIException, Unit> = Unit.right()

    override suspend fun computeAccessRightFilter(): () -> String? = { null }

    override suspend fun userCanReadEntity(entityId: URI): Either<APIException, Unit> =
        Unit.right()

    override suspend fun userCanUpdateEntity(entityId: URI): Either<APIException, Unit> =
        Unit.right()

    override suspend fun userCanAdminEntity(entityId: URI): Either<APIException, Unit> =
        Unit.right()

    override suspend fun createOwnerRight(entityId: URI): Either<APIException, Unit> =
        Unit.right()

    override suspend fun createOwnerRights(
        entitiesId: List<URI>
    ): Either<APIException, Unit> = Unit.right()

    override suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit> = Unit.right()

    override suspend fun getAuthorizedEntities(
        entitiesQuery: EntitiesQueryFromGet,
        includeDeleted: Boolean,
        contexts: List<String>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = Pair(-1, emptyList<ExpandedEntity>()).right()

    override suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = Pair(-1, emptyList<ExpandedEntity>()).right()

    override suspend fun getUsers(
        offset: Int,
        limit: Int,
        contexts: List<String>,
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = Pair(-1, emptyList<ExpandedEntity>()).right()
}
