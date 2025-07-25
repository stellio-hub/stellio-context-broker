package com.egm.stellio.search.authorization.service

import arrow.core.Either
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedEntity
import java.net.URI

interface AuthorizationService {
    suspend fun computeAccessRightFilter(): () -> String?

    suspend fun userIsAdmin(): Either<APIException, Unit>
    suspend fun userCanCreateEntities(): Either<APIException, Unit>
    suspend fun userCanReadEntity(entityId: URI): Either<APIException, Unit>
    suspend fun userCanUpdateEntity(entityId: URI): Either<APIException, Unit>
    suspend fun userCanAdminEntity(entityId: URI): Either<APIException, Unit>

    suspend fun createOwnerRight(entityId: URI): Either<APIException, Unit>
    suspend fun createOwnerRights(entitiesId: List<URI>): Either<APIException, Unit>
    suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit>

    suspend fun getAuthorizedEntities(
        entitiesQuery: EntitiesQueryFromGet,
        includeDeleted: Boolean,
        contexts: List<String>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>

    suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>

    suspend fun getUsers(
        offset: Int,
        limit: Int,
        contexts: List<String>,
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>>
}
