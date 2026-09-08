package com.egm.stellio.search.service.execution.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toBoolean
import com.egm.stellio.search.common.util.toInt
import com.egm.stellio.search.common.util.toJsonString
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.service.execution.model.ServiceExecution
import com.egm.stellio.search.service.execution.model.ServiceExecution.Companion.EXECUTION_RESULT_MEMBERS
import com.egm.stellio.search.service.execution.model.ServiceExecutionFilters
import com.egm.stellio.search.service.execution.model.ServiceExecutionStatus
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.SERVICE_EXECUTION_INVALID_UPDATE_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.serviceExecutionAlreadyExistsMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.serviceExecutionNotFoundMessage
import com.egm.stellio.shared.util.escapeSingleQuotes
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.toSqlList
import io.r2dbc.postgresql.codec.Json
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Component
class ServiceExecutionService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate
) {
    @Transactional
    suspend fun create(serviceExecution: ServiceExecution): Either<APIException, Unit> = either {
        checkExistence(serviceExecution.id, inverse = true).bind()
        upsert(serviceExecution).bind()
    }

    suspend fun merge(
        currentID: URI,
        body: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, Unit> = either {
        val currentExecution = getById(currentID).bind()
        ensure((body.keys - JSONLD_CONTEXT_KW).all(EXECUTION_RESULT_MEMBERS::contains)) {
            BadRequestDataException(SERVICE_EXECUTION_INVALID_UPDATE_MESSAGE)
        }

        val updatedExecution = currentExecution.mergeWithFragment(body, contexts).bind()

        upsert(updatedExecution).bind()
    }

    @Transactional
    suspend fun upsert(serviceExecution: ServiceExecution): Either<APIException, Unit> = either {
        serviceExecution.validate().bind()

        databaseClient.sql(
            """
            INSERT INTO service_execution(
                id, service_id, entity_id, entity_type, input, service_name,
                execution_status, progress, output, response_status_code, sub, created_at, modified_at
            )
            VALUES(
                :id, :service_id, :entity_id, :entity_type, :input, :service_name,
                :execution_status, :progress, :output, :response_status_code, :sub, :created_at, :modified_at
            )
            ON CONFLICT (id)
            DO UPDATE SET
                service_id = :service_id,
                entity_id = :entity_id,
                entity_type = :entity_type,
                input = :input,
                service_name = :service_name,
                execution_status = :execution_status,
                progress = :progress,
                output = :output,
                response_status_code = :response_status_code,
                sub = :sub,
                modified_at = :modified_at
            """.trimIndent()
        )
            .bind("id", serviceExecution.id)
            .bind("service_id", serviceExecution.serviceId)
            .bind("entity_id", serviceExecution.entityId)
            .bind("entity_type", serviceExecution.entityType)
            .bind("input", Json.of(DataTypes.serialize(serviceExecution.input)))
            .bind("service_name", serviceExecution.serviceName)
            .bind("execution_status", serviceExecution.executionStatus.name.lowercase())
            .bind("progress", serviceExecution.progress)
            .bind("output", serviceExecution.output?.let { Json.of(DataTypes.serialize(it)) })
            .bind("response_status_code", serviceExecution.responseStatusCode)
            .bind("sub", getSubFromSecurityContext())
            .bind("created_at", serviceExecution.createdAt)
            .bind("modified_at", serviceExecution.modifiedAt)
            .execute()
            .bind()
    }

    suspend fun checkExistence(
        id: URI,
        inverse: Boolean = false
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            SELECT exists (
                SELECT 1
                FROM service_execution
                WHERE id = :id
            ) as exists
            """.trimIndent()
        )
            .bind("id", id)
            .oneToResult { toBoolean(it["exists"]) }
            .flatMap { exists ->
                when {
                    exists && inverse -> AlreadyExistsException(serviceExecutionAlreadyExistsMessage(id)).left()
                    !exists && !inverse -> ResourceNotFoundException(serviceExecutionNotFoundMessage(id)).left()
                    else -> Unit.right()
                }
            }

    suspend fun getById(id: URI): Either<APIException, ServiceExecution> = either {
        checkExistence(id).bind()

        databaseClient.sql(
            """
            SELECT id, service_id, entity_id, entity_type, input, service_name,
                execution_status, progress, output, response_status_code, created_at, modified_at
            FROM service_execution
            WHERE id = :id
            """.trimIndent()
        )
            .bind("id", id)
            .oneToResult { rowToServiceExecution(it) }
            .bind()
    }

    suspend fun getServiceExecutions(
        filters: ServiceExecutionFilters = ServiceExecutionFilters(),
        limit: Int = Int.MAX_VALUE,
        offset: Int = 0,
    ): Either<APIException, List<ServiceExecution>> = either {
        val whereStatement = buildWhereStatement(filters)
        databaseClient.sql(
            """
            SELECT id, service_id, entity_id, entity_type, input, service_name,
                execution_status, progress, output, response_status_code, created_at, modified_at
            FROM service_execution
            WHERE $whereStatement
            ORDER BY created_at
            LIMIT :limit
            OFFSET :offset
            """.trimIndent()
        )
            .bind("limit", limit)
            .bind("offset", offset)
            .allToMappedList { rowToServiceExecution(it) }
    }

    suspend fun getServiceExecutionsCount(
        filters: ServiceExecutionFilters = ServiceExecutionFilters()
    ): Either<APIException, Int> =
        databaseClient.sql(
            """
            SELECT count(*)
            FROM service_execution
            WHERE ${buildWhereStatement(filters)}
            """.trimIndent()
        )
            .oneToResult { toInt(it["count"]) }

    suspend fun delete(id: URI): Either<APIException, Unit> = either {
        checkExistence(id).bind()
        r2dbcEntityTemplate.delete<ServiceExecution>()
            .matching(query(where("id").`is`(id)))
            .execute()
            .bind()
    }

    private val rowToServiceExecution: (Map<String, Any>) -> ServiceExecution = { row ->
        ServiceExecution(
            id = toUri(row["id"]),
            serviceId = toUri(row["service_id"]),
            entityId = toUri(row["entity_id"]),
            entityType = row["entity_type"] as String,
            input = DataTypes.mapper.readValue(toJsonString(row["input"]), Any::class.java),
            serviceName = row["service_name"] as? String,
            executionStatus = ServiceExecutionStatus.valueOf(
                (row["execution_status"] as String).uppercase()
            ),
            progress = row["progress"] as? Double,
            output = row["output"]?.let {
                DataTypes.mapper.readValue(toJsonString(it), Any::class.java)
            },
            responseStatusCode = row["response_status_code"] as? Int,
            createdAt = toZonedDateTime(row["created_at"]),
            modifiedAt = toZonedDateTime(row["modified_at"])
        )
    }

    companion object {
        private fun buildWhereStatement(filters: ServiceExecutionFilters): String =
            listOfNotNull(
                filters.ids?.takeIf { it.isNotEmpty() }?.let { "id IN ${it.toEscapedSqlList()}" },
                filters.serviceIds?.takeIf { it.isNotEmpty() }
                    ?.let { "service_id IN ${it.toEscapedSqlList()}" },
                filters.entityIds?.takeIf { it.isNotEmpty() }
                    ?.let { "entity_id IN ${it.toEscapedSqlList()}" },
                filters.executionStatuses?.takeIf { it.isNotEmpty() }
                    ?.map { it.name.lowercase() }
                    ?.let { "execution_status IN ${it.toEscapedSqlList()}" }
            ).ifEmpty { listOf("true") }.joinToString(" AND ")

        private fun <T> Iterable<T>.toEscapedSqlList(): String =
            map { it.toString().escapeSingleQuotes() }.toSqlList()
    }
}
