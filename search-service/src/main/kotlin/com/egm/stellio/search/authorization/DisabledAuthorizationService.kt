package com.egm.stellio.search.authorization

import arrow.core.Either
import arrow.core.Option
import arrow.core.right
import com.egm.stellio.shared.model.APIException
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

    override suspend fun userIsAdminOfEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        Unit.right()

    override suspend fun createAdminLink(entityId: URI, sub: Option<Sub>): Either<APIException, Unit> =
        Unit.right()

    override suspend fun createAdminLinks(
        entitiesId: List<URI>,
        sub: Option<Sub>
    ): Either<APIException, Unit> = Unit.right()
}
