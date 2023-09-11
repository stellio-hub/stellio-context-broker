package com.egm.stellio.search.authorization

import arrow.core.Either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams
import java.net.URI

interface AuthorizationService {
    suspend fun computeAccessRightFilter(): () -> String?

    suspend fun userIsAdmin(): Either<APIException, Unit>
    suspend fun userCanCreateEntities(): Either<APIException, Unit>
    suspend fun userCanReadEntity(entityId: URI): Either<APIException, Unit>
    suspend fun userCanUpdateEntity(entityId: URI): Either<APIException, Unit>
    suspend fun userCanAdminEntity(entityId: URI): Either<APIException, Unit>

    suspend fun createAdminRight(entityId: URI): Either<APIException, Unit>
    suspend fun createAdminRights(entitiesId: List<URI>): Either<APIException, Unit>
    suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit>

    suspend fun getAuthorizedEntities(
        queryParams: QueryParams,
        contextLink: String
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>>

    suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contextLink: String
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>>

    suspend fun getUsers(
        offset: Int,
        limit: Int,
        contextLink: String,
    ): Either<APIException, Pair<Int, List<JsonLdEntity>>>
}
