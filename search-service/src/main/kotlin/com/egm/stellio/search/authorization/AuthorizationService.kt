package com.egm.stellio.search.authorization

import arrow.core.Either
import arrow.core.Option
import com.egm.stellio.search.model.EntitiesQuery
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.Sub
import java.net.URI

interface AuthorizationService {
    suspend fun computeAccessRightFilter(sub: Option<Sub>): () -> String?

    suspend fun userIsAdmin(sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanCreateEntities(sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanReadEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanAdminEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>

    suspend fun createAdminRight(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun createAdminRights(entitiesId: List<URI>, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit>

    suspend fun getAuthorizedEntities(
        entitiesQuery: EntitiesQuery,
        contextLink: String,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>

    suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contextLink: String,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>

    suspend fun getUsers(
        offset: Int,
        limit: Int,
        contextLink: String,
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>
}
