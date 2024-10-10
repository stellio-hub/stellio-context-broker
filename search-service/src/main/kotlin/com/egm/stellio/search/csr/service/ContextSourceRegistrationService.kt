package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.Option
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toBoolean
import com.egm.stellio.search.common.util.toInt
import com.egm.stellio.search.common.util.toOptionalZonedDateTime
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.ContextSourceRegistration.RegistrationInfo
import com.egm.stellio.search.csr.model.ContextSourceRegistration.TimeInterval
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.shared.util.toStringValue
import io.r2dbc.postgresql.codec.Json
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Component
class ContextSourceRegistrationService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate
) {

    @Transactional
    suspend fun create(
        contextSourceRegistration: ContextSourceRegistration,
        sub: Option<Sub>
    ): Either<APIException, Unit> = either {
        contextSourceRegistration.validate().bind()
        checkExistence(contextSourceRegistration.id, true).bind()

        val insertStatement =
            """
            INSERT INTO context_source_registration(
                id,
                endpoint,
                mode,
                information,
                operations,
                observation_interval_start,
                observation_interval_end,
                management_interval_start,
                management_interval_end,
                sub,
                created_at
            )
            VALUES(
                :id,
                :endpoint,
                :mode,
                :information,
                :operations,
                :observation_interval_start,
                :observation_interval_end,
                :management_interval_start,
                :management_interval_end,
                :sub,
                :created_at
            )
            """.trimIndent()
        databaseClient.sql(insertStatement)
            .bind("id", contextSourceRegistration.id)
            .bind("endpoint", contextSourceRegistration.endpoint)
            .bind("mode", contextSourceRegistration.mode.key)
            .bind(
                "information",
                Json.of(mapper.writeValueAsString(contextSourceRegistration.information))
            )
            .bind("operations", contextSourceRegistration.operations.map { it.key }.toTypedArray())
            .bind("observation_interval_start", contextSourceRegistration.observationInterval?.start)
            .bind("observation_interval_end", contextSourceRegistration.observationInterval?.end)
            .bind("management_interval_start", contextSourceRegistration.managementInterval?.start)
            .bind("management_interval_end", contextSourceRegistration.managementInterval?.end)
            .bind("sub", sub.toStringValue())
            .bind("created_at", contextSourceRegistration.createdAt)
            .execute().bind()
    }

    suspend fun checkExistence(
        id: URI,
        inverse: Boolean = false
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            SELECT exists (
                SELECT 1
                FROM context_source_registration
                WHERE id = :id
            ) as exists
            """.trimIndent()
        ).bind("id", id)
            .oneToResult { toBoolean(it["exists"]) }
            .flatMap {
                if (it && !inverse)
                    Unit.right()
                else if (!it && inverse)
                    Unit.right()
                else if (it)
                    AlreadyExistsException(ContextSourceRegistration.alreadyExistsMessage(id)).left()
                else
                    ResourceNotFoundException(ContextSourceRegistration.notFoundMessage(id)).left()
            }

    suspend fun getById(id: URI): Either<APIException, ContextSourceRegistration> = either {
        checkExistence(id).bind()

        val selectStatement =
            """
            SELECT id,
                endpoint,
                mode,
                information,
                operations,
                observation_interval_start,
                observation_interval_end,
                management_interval_start,
                management_interval_end,
                created_at,
                modified_at
            FROM context_source_registration  
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .oneToResult { rowToContextSourceRegistration(it) }
    }

    suspend fun isCreatorOf(id: URI, sub: Option<Sub>): Either<APIException, Boolean> {
        val selectStatement =
            """
            SELECT sub
            FROM context_source_registration
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .oneToResult {
                it["sub"] == sub.toStringValue()
            }
    }

    suspend fun delete(id: URI): Either<APIException, Unit> = either {
        checkExistence(id).bind()
        r2dbcEntityTemplate.delete(ContextSourceRegistration::class.java)
            .matching(query(where("id").`is`(id)))
            .execute()
    }

    suspend fun getContextSourceRegistrations(
        sub: Option<Sub>,
        filters: CSRFilters = CSRFilters(),
        limit: Int = Int.MAX_VALUE,
        offset: Int = 0,
    ): List<ContextSourceRegistration> {
        val filterQuery = filters.buildWHEREStatement()

        val selectStatement =
            """
            SELECT csr.id,
                endpoint,
                mode,
                information,
                operations,
                observation_interval_start,
                observation_interval_end,
                management_interval_start,
                management_interval_end,
                created_at,
                modified_at
            FROM context_source_registration as csr
            LEFT JOIN jsonb_to_recordset(information) 
                as information(entities jsonb,propertyNames text[],relationshipNames text[] ) on true
            LEFT JOIN jsonb_to_recordset(entities) 
                as entity_info(id text,"idPattern" text,"type" text) on true
            WHERE sub = :sub AND $filterQuery
            GROUP BY csr.id
            ORDER BY csr.id
            LIMIT :limit
            OFFSET :offset
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("limit", limit)
            .bind("offset", offset)
            .bind("sub", sub.toStringValue())
            .allToMappedList { rowToContextSourceRegistration(it) }
    }

    suspend fun getContextSourceRegistrationsCount(sub: Option<Sub>): Either<APIException, Int> {
        val selectStatement =
            """
            SELECT count(*)
            FROM context_source_registration
            WHERE sub = :sub
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("sub", sub.toStringValue())
            .oneToResult { toInt(it["count"]) }
    }

    private val rowToContextSourceRegistration: ((Map<String, Any>) -> ContextSourceRegistration) = { row ->
        ContextSourceRegistration(
            id = toUri(row["id"]),
            endpoint = toUri(row["endpoint"]),
            mode = Mode.fromString(row["mode"] as? String),
            information = mapper.readerForListOf(RegistrationInfo::class.java)
                .readValue((row["information"] as Json).asString()),
            operations = (row["operations"] as Array<String>).mapNotNull { Operation.fromString(it) },
            createdAt = toZonedDateTime(row["created_at"]),
            modifiedAt = toOptionalZonedDateTime(row["modified_at"]),
            observationInterval = row["observation_interval_start"]?.let {
                TimeInterval(
                    toZonedDateTime(it),
                    toOptionalZonedDateTime(row["observation_interval_end"])
                )
            },
            managementInterval = row["management_interval_start"]?.let {
                TimeInterval(
                    toZonedDateTime(it),
                    toOptionalZonedDateTime(row["management_interval_end"])
                )
            },
        )
    }
}
