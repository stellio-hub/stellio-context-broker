package com.egm.stellio.search.authorization

import arrow.core.Either
import arrow.core.Option
import arrow.core.right
import com.egm.stellio.search.model.EntitiesQuery
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.Sub
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class DisabledAuthorizationService : AuthorizationService {
    override suspend fun userIsAdmin(sub: Option<Sub>): Either<APIException, Unit> = Unit.right()

    override suspend fun userCanCreateEntities(sub: Option<Sub>): Either<APIException, Unit> = Unit.right()

    override suspend fun computeAccessRightFilter(sub: Option<Sub>): () -> String? = { null }

    override suspend fun userCanReadEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        Unit.right()

    override suspend fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        Unit.right()

    override suspend fun userCanAdminEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        Unit.right()

    override suspend fun createCreatorRight(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        Unit.right()

    override suspend fun createCreatorRights(
        entitiesId: List<URI>,
        sub: Option<Sub>
    ): Either<APIException, Unit> = Unit.right()

    override suspend fun removeRightsOnEntity(entityId: URI): Either<APIException, Unit> = Unit.right()

    override suspend fun getAuthorizedEntities(
        entitiesQuery: EntitiesQuery,
        contexts: List<String>,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = Pair(-1, emptyList<ExpandedEntity>()).right()

    override suspend fun getGroupsMemberships(
        offset: Int,
        limit: Int,
        contexts: List<String>,
        sub: Option<Sub>
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = Pair(-1, emptyList<ExpandedEntity>()).right()

    override suspend fun getUsers(
        offset: Int,
        limit: Int,
        contexts: List<String>,
    ): Either<APIException, Pair<Int, List<ExpandedEntity>>> = Pair(-1, emptyList<ExpandedEntity>()).right()
}
