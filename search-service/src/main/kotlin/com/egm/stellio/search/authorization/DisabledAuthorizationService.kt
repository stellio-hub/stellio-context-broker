package com.egm.stellio.search.authorization

import arrow.core.Either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams
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

    override suspend fun createAdminRight(entityId: URI): Either<APIException, Unit> =
        Unit.right()

    override suspend fun createAdminRights(entitiesId: List<URI>): Either<APIException, Unit> = Unit.right()

    override suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit> = Unit.right()

    override suspend fun getAuthorizedEntities(
        queryParams: QueryParams,
        contextLink: String
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>> = Pair(-1, emptyList<JsonLdEntity>()).right()

    override suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contextLink: String
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>> = Pair(-1, emptyList<JsonLdEntity>()).right()

    override suspend fun getUsers(
        offset: Int,
        limit: Int,
        contextLink: String,
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>> = Pair(-1, emptyList<JsonLdEntity>()).right()
}
