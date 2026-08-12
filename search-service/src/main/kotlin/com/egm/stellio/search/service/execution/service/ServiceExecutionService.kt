package com.egm.stellio.search.service.execution.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toBoolean
import com.egm.stellio.search.common.util.toJsonString
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.service.execution.model.ServiceExecution
import com.egm.stellio.search.service.execution.model.ServiceExecutionStatus
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.serviceExecutionAlreadyExistsMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecutionErrorMessages.serviceExecutionNotFoundMessage
import com.egm.stellio.shared.util.getSubFromSecurityContext
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

    @Transactional
    suspend fun upsert(serviceExecution: ServiceExecution): Either<APIException, Unit> = either {
        serviceExecution.validate().bind()

        databaseClient.sql(
            """
            INSERT INTO service_execution(
                id, service_id, entity_id, entity_type, input, service_name,
                execution_status, completion, output, sub, created_at, modified_at
            )
            VALUES(
                :id, :service_id, :entity_id, :entity_type, :input, :service_name,
                :execution_status, :completion, :output, :sub, :created_at, :modified_at
            )
            ON CONFLICT (id)
            DO UPDATE SET
                service_id = :service_id,
                entity_id = :entity_id,
                entity_type = :entity_type,
                input = :input,
                service_name = :service_name,
                execution_status = :execution_status,
                completion = :completion,
                output = :output,
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
            .bind("completion", serviceExecution.completion)
            .bind("output", serviceExecution.output?.let { Json.of(DataTypes.serialize(it)) })
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
                execution_status, completion, output, created_at, modified_at
            FROM service_execution
            WHERE id = :id
            """.trimIndent()
        )
            .bind("id", id)
            .oneToResult { rowToServiceExecution(it) }
            .bind()
    }

    suspend fun delete(id: URI): Either<APIException, Unit> = either {
        checkExistence(id).bind()
        r2dbcEntityTemplate.delete<ServiceExecution>()
            .matching(query(where("id").`is`(id)))
            .execute()
            .bind()
    }

    companion object {
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
                completion = row["completion"] as? Double,
                output = row["output"]?.let {
                    DataTypes.mapper.readValue(toJsonString(it), Any::class.java)
                },
                createdAt = toZonedDateTime(row["created_at"]),
                modifiedAt = toZonedDateTime(row["modified_at"])
            )
        }
    }
}
