package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.EntityPayload
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedEntity
import org.springframework.stereotype.Service
import java.net.URI

@Service
class EntityQueryService(
    private val entityPayloadService: EntityPayloadService,
) {
    suspend fun queryEntity(entityId: URI): Either<APIException, ExpandedEntity> =
        either {
            val entityPayload = entityPayloadService.retrieve(entityId).bind()
            toJsonLdEntity(entityPayload)
        }

    suspend fun queryEntities(
        entitiesQuery: EntitiesQuery,
        accessRightFilter: () -> String?
    ): Either<APIException, Pair<List<ExpandedEntity>, Int>> = either {
        val entitiesIds = entityPayloadService.queryEntities(entitiesQuery, accessRightFilter)
        val count = entityPayloadService.queryEntitiesCount(entitiesQuery, accessRightFilter).bind()

        // we can have an empty list of entities with a non-zero count (e.g., offset too high)
        if (entitiesIds.isEmpty())
            return@either Pair<List<ExpandedEntity>, Int>(emptyList(), count)

        val entitiesPayloads = entityPayloadService.retrieve(entitiesIds).map { toJsonLdEntity(it) }

        Pair(entitiesPayloads, count).right().bind()
    }

    private fun toJsonLdEntity(entityPayload: EntityPayload): ExpandedEntity {
        val deserializedEntity = entityPayload.payload.deserializeAsMap()
        return ExpandedEntity(deserializedEntity)
    }
}
