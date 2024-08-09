package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedEntity
import org.springframework.stereotype.Service
import java.net.URI

@Service
class EntityQueryService(
    private val entityService: EntityService,
) {
    suspend fun queryEntity(entityId: URI): Either<APIException, ExpandedEntity> =
        either {
            val entityPayload = entityService.retrieve(entityId).bind()
            toJsonLdEntity(entityPayload)
        }

    suspend fun queryEntities(
        entitiesQuery: EntitiesQuery,
        accessRightFilter: () -> String?
    ): Either<APIException, Pair<List<ExpandedEntity>, Int>> = either {
        val entitiesIds = entityService.queryEntities(entitiesQuery, accessRightFilter)
        val count = entityService.queryEntitiesCount(entitiesQuery, accessRightFilter).bind()

        // we can have an empty list of entities with a non-zero count (e.g., offset too high)
        if (entitiesIds.isEmpty())
            return@either Pair<List<ExpandedEntity>, Int>(emptyList(), count)

        val entitiesPayloads = entityService.retrieve(entitiesIds).map { toJsonLdEntity(it) }

        Pair(entitiesPayloads, count).right().bind()
    }

    private fun toJsonLdEntity(entity: Entity): ExpandedEntity {
        val deserializedEntity = entity.payload.deserializeAsMap()
        return ExpandedEntity(deserializedEntity)
    }
}
