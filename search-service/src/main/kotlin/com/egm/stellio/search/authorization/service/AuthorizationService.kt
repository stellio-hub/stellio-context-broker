package com.egm.stellio.search.authorization.service

import arrow.core.Either
import arrow.core.Option
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
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

    suspend fun createOwnerRight(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun createOwnerRights(entitiesId: List<URI>, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit>

    suspend fun getAuthorizedEntities(
        entitiesQuery: EntitiesQueryFromGet,
        includeDeleted: Boolean,
        contexts: List<String>,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>

    suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contexts: List<String>,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>

    suspend fun getUsers(
        offset: Int,
        limit: Int,
        contexts: List<String>,
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>
}
