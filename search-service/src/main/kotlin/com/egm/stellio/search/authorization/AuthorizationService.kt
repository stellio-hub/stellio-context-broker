package com.egm.stellio.search.authorization

import arrow.core.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.Sub
import java.net.URI

interface AuthorizationService {
    suspend fun computeAccessRightFilter(sub: Option<Sub>): () -> String?

    // FIXME should not be exposed
    suspend fun userIsAdmin(sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanCreateEntities(sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanReadEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userIsAdminOfEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun createAdminLink(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun createAdminLinks(entitiesId: List<URI>, sub: Option<Sub>): Either<APIException, Unit>
}
