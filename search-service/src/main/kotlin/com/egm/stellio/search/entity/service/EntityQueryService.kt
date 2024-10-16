package com.egm.stellio.search.entity.service

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.common.util.*
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.util.rowToEntity
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import java.net.URI

@Service
class EntityQueryService(
    private val databaseClient: DatabaseClient,
    private val authorizationService: AuthorizationService
) {
    suspend fun queryEntity(
        entityId: URI,
        sub: Sub? = null
    ): Either<APIException, ExpandedEntity> = either {
        checkEntityExistence(entityId).bind()
        authorizationService.userCanReadEntity(entityId, sub.toOption()).bind()

        val entityPayload = retrieve(entityId).bind()
        toJsonLdEntity(entityPayload)
    }

    suspend fun queryEntities(
        entitiesQuery: EntitiesQuery,
        sub: Sub? = null
    ): Either<APIException, Pair<List<ExpandedEntity>, Int>> = either {
        val accessRightFilter = authorizationService.computeAccessRightFilter(sub.toOption())

        val entitiesIds = queryEntities(entitiesQuery, accessRightFilter)
        val count = queryEntitiesCount(entitiesQuery, accessRightFilter).bind()

        // we can have an empty list of entities with a non-zero count (e.g., offset too high)
        if (entitiesIds.isEmpty())
            return@either Pair<List<ExpandedEntity>, Int>(emptyList(), count)

        val entitiesPayloads = retrieve(entitiesIds).map { toJsonLdEntity(it) }

        Pair(entitiesPayloads, count).right().bind()
    }

    private fun toJsonLdEntity(entity: Entity): ExpandedEntity {
        val deserializedEntity = entity.payload.deserializeAsMap()
        return ExpandedEntity(deserializedEntity)
    }

    suspend fun queryEntities(
        entitiesQuery: EntitiesQuery,
        accessRightFilter: () -> String?
    ): List<URI> {
        val filterQuery = buildFullEntitiesFilter(entitiesQuery, accessRightFilter)

        val selectQuery =
            """
            SELECT DISTINCT(entity_payload.entity_id)
            FROM entity_payload
            LEFT JOIN temporal_entity_attribute tea
            ON tea.entity_id = entity_payload.entity_id
            WHERE $filterQuery
            ORDER BY entity_id
            LIMIT :limit
            OFFSET :offset   
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("limit", entitiesQuery.paginationQuery.limit)
            .bind("offset", entitiesQuery.paginationQuery.offset)
            .allToMappedList { toUri(it["entity_id"]) }
    }

    suspend fun queryEntitiesCount(
        entitiesQuery: EntitiesQuery,
        accessRightFilter: () -> String?
    ): Either<APIException, Int> {
        val filterQuery = buildFullEntitiesFilter(entitiesQuery, accessRightFilter)

        val countQuery =
            """
            SELECT count(distinct(entity_payload.entity_id)) as count_entity
            FROM entity_payload
            LEFT JOIN temporal_entity_attribute tea
            ON tea.entity_id = entity_payload.entity_id
            WHERE $filterQuery
            """.trimIndent()

        return databaseClient
            .sql(countQuery)
            .oneToResult { it["count_entity"] as Long }
            .map { it.toInt() }
    }

    private fun buildFullEntitiesFilter(entitiesQuery: EntitiesQuery, accessRightFilter: () -> String?): String =
        buildEntitiesQueryFilter(
            entitiesQuery,
            accessRightFilter
        ).let {
            if (entitiesQuery.q != null)
                it.wrapToAndClause(buildQQuery(entitiesQuery.q, entitiesQuery.contexts))
            else it
        }.let {
            if (entitiesQuery.scopeQ != null)
                it.wrapToAndClause(buildScopeQQuery(entitiesQuery.scopeQ))
            else it
        }.let {
            if (entitiesQuery.geoQuery != null)
                it.wrapToAndClause(buildGeoQuery(entitiesQuery.geoQuery))
            else it
        }

    fun buildEntitiesQueryFilter(
        entitiesQuery: EntitiesQuery,
        accessRightFilter: () -> String?
    ): String {
        val formattedIds =
            if (entitiesQuery.ids.isNotEmpty())
                entitiesQuery.ids.joinToString(
                    separator = ",",
                    prefix = "entity_payload.entity_id in(",
                    postfix = ")"
                ) { "'$it'" }
            else null
        val formattedIdPattern =
            if (!entitiesQuery.idPattern.isNullOrEmpty())
                "entity_payload.entity_id ~ '${entitiesQuery.idPattern}'"
            else null
        val formattedType = entitiesQuery.typeSelection?.let { "(" + buildTypeQuery(it) + ")" }
        val formattedAttrs =
            if (entitiesQuery.attrs.isNotEmpty())
                entitiesQuery.attrs.joinToString(
                    separator = ",",
                    prefix = "attribute_name in (",
                    postfix = ")"
                ) { "'$it'" }
            else null

        val queryFilter =
            listOfNotNull(
                formattedIds,
                formattedIdPattern,
                formattedType,
                formattedAttrs,
                accessRightFilter()
            )

        return queryFilter.joinToString(separator = " AND ")
    }

    suspend fun retrieve(entityId: URI): Either<APIException, Entity> =
        databaseClient.sql(
            """
            SELECT * from entity_payload
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .oneToResult { it.rowToEntity() }

    suspend fun retrieve(entitiesIds: List<URI>): List<Entity> =
        databaseClient.sql(
            """
            SELECT * from entity_payload
            WHERE entity_id IN (:entities_ids)
            """.trimIndent()
        )
            .bind("entities_ids", entitiesIds)
            .allToMappedList { it.rowToEntity() }

    suspend fun checkEntityExistence(
        entityId: URI,
        inverse: Boolean = false
    ): Either<APIException, Unit> {
        val selectQuery =
            """
            select 
                exists(
                    select 1 
                    from entity_payload 
                    where entity_id = :entity_id
                ) as entityExists;
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .oneToResult { it["entityExists"] as Boolean }
            .flatMap {
                if (it && !inverse || !it && inverse)
                    Unit.right()
                else if (it)
                    AlreadyExistsException(entityAlreadyExistsMessage(entityId.toString())).left()
                else
                    ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()
            }
    }

    suspend fun filterExistingEntitiesAsIds(entitiesIds: List<URI>): List<URI> {
        if (entitiesIds.isEmpty()) {
            return emptyList()
        }

        val query =
            """
            select entity_id 
            from entity_payload
            where entity_id in (:entities_ids)
            """.trimIndent()

        return databaseClient
            .sql(query)
            .bind("entities_ids", entitiesIds)
            .allToMappedList { toUri(it["entity_id"]) }
    }
}
