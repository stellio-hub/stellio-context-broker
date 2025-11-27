package com.egm.stellio.search.csr.service

import arrow.core.Either
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
import com.egm.stellio.search.csr.model.ContextSourceRegistration.TimeInterval
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.search.csr.model.RegistrationInfoDBWriter
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.DateUtils.ngsiLdDateTime
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.buildTypeQuery
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.toSqlArray
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.data.relational.core.query.Update
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
        csr: ContextSourceRegistration,
    ): Either<APIException, Unit> = either {
        checkExistence(csr.id, true).bind()
        upsert(csr).bind()
    }

    @Transactional
    suspend fun upsert(
        csr: ContextSourceRegistration
    ): Either<APIException, Unit> = either {
        csr.validate().bind()

        val insertStatement =
            """
            INSERT INTO context_source_registration(
                id, endpoint, mode,
                information, operations,  registration_name,
                observation_interval_start, observation_interval_end,
                management_interval_start, management_interval_end,
                sub, created_at, modified_at
            )
            VALUES(
                :id, :endpoint, :mode,
                :information, :operations, :registration_name,
                :observation_interval_start, :observation_interval_end,
                :management_interval_start, :management_interval_end,
                :sub, :created_at, :modified_at
            )
            ON CONFLICT (id)
            DO UPDATE SET
                endpoint = :endpoint,
                mode = :mode,
                information = :information,
                operations = :operations,
                registration_name = :registration_name,
                observation_interval_start = :observation_interval_start,
                observation_interval_end = :observation_interval_end,
                management_interval_start = :management_interval_start,
                management_interval_end = :management_interval_end,
                sub = :sub,
                modified_at = :modified_at
            """.trimIndent()
        databaseClient.sql(insertStatement)
            .bind("id", csr.id)
            .bind("endpoint", csr.endpoint)
            .bind("mode", csr.mode.key)
            .bind(
                "information",
                Json.of(
                    DataTypes.mapper.writeValueAsString(
                        csr.information.map { RegistrationInfoDBWriter(it) }
                    )
                )
            )
            .bind("operations", csr.operations.map { it.key }.toTypedArray())
            .bind("registration_name", csr.registrationName)
            .bind("observation_interval_start", csr.observationInterval?.start)
            .bind("observation_interval_end", csr.observationInterval?.end)
            .bind("management_interval_start", csr.managementInterval?.start)
            .bind("management_interval_end", csr.managementInterval?.end)
            .bind("sub", getSubFromSecurityContext())
            .bind("created_at", csr.createdAt)
            .bind("modified_at", csr.modifiedAt)
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
                registration_name,
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

    suspend fun isCreatorOf(id: URI, sub: Sub): Either<APIException, Boolean> {
        val selectStatement =
            """
            SELECT sub
            FROM context_source_registration
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .oneToResult {
                it["sub"] == sub
            }
    }

    suspend fun delete(id: URI): Either<APIException, Unit> = either {
        checkExistence(id).bind()
        r2dbcEntityTemplate.delete(ContextSourceRegistration::class.java)
            .matching(query(where("id").`is`(id)))
            .execute()
    }

    suspend fun getContextSourceRegistrations(
        filters: CSRFilters = CSRFilters(),
        limit: Int = Int.MAX_VALUE,
        offset: Int = 0,
    ): List<ContextSourceRegistration> {
        val filterQuery = buildWhereStatement(filters)

        val selectStatement =
            """
            SELECT csr.id,
                endpoint,
                mode,
                information,
                operations,
                registration_name,
                observation_interval_start,
                observation_interval_end,
                management_interval_start,
                management_interval_end,
                created_at,
                modified_at
            FROM context_source_registration as csr
            LEFT JOIN jsonb_to_recordset(information)
                as information(entities jsonb, "propertyNames" text[], "relationshipNames" text[]) on true
            LEFT JOIN jsonb_to_recordset(entities)
                as entity_info(id text, "idPattern" text, type text[]) on true
            WHERE $filterQuery
            GROUP BY csr.id
            ORDER BY csr.id
            LIMIT :limit
            OFFSET :offset
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("limit", limit)
            .bind("offset", offset)
            .allToMappedList { rowToContextSourceRegistration(it) }
    }

    suspend fun getContextSourceRegistrationsCount(
        filters: CSRFilters = CSRFilters(),
    ): Either<APIException, Int> {
        val filterQuery = buildWhereStatement(filters)

        val selectStatement =
            """
            SELECT count(distinct csr.id)
            FROM context_source_registration as csr
            LEFT JOIN jsonb_to_recordset(information)
                as information(entities jsonb, "propertyNames" text[], "relationshipNames" text[]) on true
            LEFT JOIN jsonb_to_recordset(entities)
                as entity_info(id text, "idPattern" text, type text[]) on true
            WHERE $filterQuery
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .oneToResult { toInt(it["count"]) }
    }

    suspend fun updateContextSourceStatus(
        csr: ContextSourceRegistration,
        success: Boolean
    ) {
        val updateStatement = if (success)
            Update.update("status", ContextSourceRegistration.StatusType.OK.name)
                .set("times_sent", csr.timesSent + 1)
                .set("last_success", ngsiLdDateTime())
        else Update.update("status", ContextSourceRegistration.StatusType.FAILED.name)
            .set("times_sent", csr.timesSent + 1)
            .set("times_failed", csr.timesFailed + 1)
            .set("last_failure", ngsiLdDateTime())

        r2dbcEntityTemplate.update(
            query(where("id").`is`(csr.id)),
            updateStatement,
            ContextSourceRegistration::class.java
        ).awaitFirst()
    }

    companion object {
        private val operationRegex = "${ContextSourceRegistration::operations.name}==([a-zA-Z]+)"
        private val validationRegex = "($operationRegex\\|?)+\$".toRegex()

        private fun buildWhereStatement(csrFilters: CSRFilters): String {
            val idFilter = if (csrFilters.ids.isNotEmpty())
                """
            (
                entity_info.id is null OR
                entity_info.id in ('${csrFilters.ids.joinToString("', '")}')
            ) AND
            (
                entity_info."idPattern" is null OR 
                ${csrFilters.ids.joinToString(" OR ") { "'$it' ~ entity_info.\"idPattern\"" }}
            )
                """.trimIndent()
            else null
            val typeFilter = if (!csrFilters.typeSelection.isNullOrBlank()) {
                val typeQuery = buildTypeQuery(csrFilters.typeSelection, columnName = "type")
                """
                (
                    type is null OR
                    ( $typeQuery )
                )
                """.trimIndent()
            } else null

            // we only filter on id since there is no easy way to know if two idPatterns overlap
            // possible resources : https://meta.stackoverflow.com/questions/426313/canonical-for-overlapping-regex-questions
            val idPatternFilter = if (!csrFilters.idPattern.isNullOrBlank())
                """
                (
                    entity_info.id is null OR
                    entity_info.id ~ ('${csrFilters.idPattern}')
                )
                """.trimIndent()
            else null

            val csfFilter = if (csrFilters.csf != null && validationRegex.matches(csrFilters.csf)) {
                val operations = operationRegex.toRegex().findAll(csrFilters.csf).map { it.groups[1]?.value }
                "operations && ${operations.toSqlArray()}"
            } else null

            val attrsFilter = if (csrFilters.attrs.isNotEmpty()) {
                val attrsArray = csrFilters.attrs.toSqlArray()
                """
                (
                    (information."relationshipNames" is null AND information."propertyNames" is null) OR 
                    information."relationshipNames" && $attrsArray OR 
                    information."propertyNames" && $attrsArray 
                )
                """.trimIndent()
            } else null

            val filters = listOfNotNull(idFilter, typeFilter, idPatternFilter, csfFilter, attrsFilter)

            return if (filters.isEmpty()) "true" else filters.joinToString(" AND ")
        }

        private val rowToContextSourceRegistration: ((Map<String, Any>) -> ContextSourceRegistration) = { row ->
            ContextSourceRegistration(
                id = toUri(row["id"]),
                endpoint = toUri(row["endpoint"]),
                mode = Mode.fromString(row["mode"] as? String),
                information = DataTypes.mapper.readerForListOf(RegistrationInfo::class.java)
                    .readValue((row["information"] as Json).asString()),
                operations = (row["operations"] as Array<String>).mapNotNull { Operation.fromString(it) },
                registrationName = row["registration_name"] as? String,
                createdAt = toZonedDateTime(row["created_at"]),
                modifiedAt = toZonedDateTime(row["modified_at"]),
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
}
