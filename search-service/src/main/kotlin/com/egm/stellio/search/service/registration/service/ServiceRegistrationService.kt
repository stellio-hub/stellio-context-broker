package com.egm.stellio.search.service.registration.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.model.UnparsedGeoQuery
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toBoolean
import com.egm.stellio.search.common.util.toInt
import com.egm.stellio.search.common.util.toJsonString
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.csr.model.EntityInfoDB
import com.egm.stellio.search.service.registration.model.ServiceInformation
import com.egm.stellio.search.service.registration.model.ServiceRegistration
import com.egm.stellio.search.service.registration.model.ServiceRegistrationFilters
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.ErrorMessages.ServiceRegistration.serviceRegistrationAlreadyExistsMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceRegistration.serviceRegistrationNotFoundMessage
import com.egm.stellio.shared.util.buildTypeQuery
import com.egm.stellio.shared.util.escapeSingleQuotes
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.toSqlList
import io.r2dbc.postgresql.codec.Json
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.http.HttpMethod
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Component
class ServiceRegistrationService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate
) {
    @Transactional
    suspend fun create(serviceRegistration: ServiceRegistration): Either<APIException, Unit> = either {
        checkExistence(serviceRegistration.id, inverse = true).bind()
        upsert(serviceRegistration).bind()
    }

    @Transactional
    suspend fun upsert(serviceRegistration: ServiceRegistration): Either<APIException, Unit> = either {
        serviceRegistration.validate().bind()

        databaseClient.sql(
            """
            INSERT INTO service_registration(
                id, endpoint, endpoint_method, entities, service_information,
                q, geo_q, scope_q, sub, created_at, modified_at
            )
            VALUES(
                :id, :endpoint, :endpoint_method, :entities, :service_information,
                :q, :geo_q, :scope_q, :sub, :created_at, :modified_at
            )
            ON CONFLICT (id)
            DO UPDATE SET
                endpoint = :endpoint,
                endpoint_method = :endpoint_method,
                entities = :entities,
                service_information = :service_information,
                q = :q,
                geo_q = :geo_q,
                scope_q = :scope_q,
                sub = :sub,
                modified_at = :modified_at
            """.trimIndent()
        )
            .bind("id", serviceRegistration.id)
            .bind("endpoint", serviceRegistration.endpoint)
            .bind("endpoint_method", serviceRegistration.endpointMethod.name())
            .bind(
                "entities",
                Json.of(DataTypes.serialize(serviceRegistration.entities.map(::EntityInfoDB)))
            )
            .bind("service_information", Json.of(DataTypes.serialize(serviceRegistration.serviceInformation)))
            .bind("q", serviceRegistration.q)
            .bind("geo_q", serviceRegistration.geoQ?.let { Json.of(DataTypes.serialize(it)) })
            .bind("scope_q", serviceRegistration.scopeQ)
            .bind("sub", getSubFromSecurityContext())
            .bind("created_at", serviceRegistration.createdAt)
            .bind("modified_at", serviceRegistration.modifiedAt)
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
                FROM service_registration
                WHERE id = :id
            ) as exists
            """.trimIndent()
        )
            .bind("id", id)
            .oneToResult { toBoolean(it["exists"]) }
            .flatMap { exists ->
                when {
                    exists && inverse -> AlreadyExistsException(serviceRegistrationAlreadyExistsMessage(id)).left()
                    !exists && !inverse -> ResourceNotFoundException(serviceRegistrationNotFoundMessage(id)).left()
                    else -> Unit.right()
                }
            }

    suspend fun getById(id: URI): Either<APIException, ServiceRegistration> = either {
        checkExistence(id).bind()

        databaseClient.sql(
            """
            SELECT id, endpoint, endpoint_method, entities, service_information,
                q, geo_q, scope_q, created_at, modified_at
            FROM service_registration
            WHERE id = :id
            """.trimIndent()
        )
            .bind("id", id)
            .oneToResult { rowToServiceRegistration(it) }
            .bind()
    }

    suspend fun delete(id: URI): Either<APIException, Unit> = either {
        checkExistence(id).bind()
        r2dbcEntityTemplate.delete<ServiceRegistration>()
            .matching(query(where("id").`is`(id)))
            .execute()
            .bind()
    }

    suspend fun getServiceRegistrations(
        filters: ServiceRegistrationFilters,
        limit: Int = Int.MAX_VALUE,
        offset: Int = 0
    ): List<ServiceRegistration> {
        val whereStatement = buildWhereStatement(filters)

        return databaseClient.sql(
            """
            SELECT registration.id, endpoint, endpoint_method, entities, service_information,
                q, geo_q, scope_q, created_at, modified_at
            FROM service_registration AS registration
            LEFT JOIN jsonb_to_recordset(entities)
                AS entity_info(id text, "idPattern" text, type text[]) ON true
            WHERE $whereStatement
            GROUP BY registration.id
            ORDER BY registration.id
            LIMIT :limit
            OFFSET :offset
            """.trimIndent()
        )
            .bind("limit", limit)
            .bind("offset", offset)
            .allToMappedList { rowToServiceRegistration(it) }
    }

    suspend fun getServiceRegistrationsCount(
        filters: ServiceRegistrationFilters
    ): Either<APIException, Int> {
        val whereStatement = buildWhereStatement(filters)

        return databaseClient.sql(
            """
            SELECT count(distinct registration.id)
            FROM service_registration AS registration
            LEFT JOIN jsonb_to_recordset(entities)
                AS entity_info(id text, "idPattern" text, type text[]) ON true
            WHERE $whereStatement
            """.trimIndent()
        )
            .oneToResult { toInt(it["count"]) }
    }

    companion object {
        private fun buildWhereStatement(filters: ServiceRegistrationFilters): String {
            val escapedIds = filters.ids.map { it.toString().escapeSingleQuotes() }
            val idFilter =
                """
                (
                    entity_info.id IS NULL OR
                    entity_info.id IN ${escapedIds.toSqlList()}
                ) AND (
                    entity_info.id IS NOT NULL OR
                    entity_info."idPattern" IS NULL OR
                    ${escapedIds.joinToString(" OR ") { "'$it' ~ entity_info.\"idPattern\"" }}
                )
                """.trimIndent()

            val typeFilter = buildTypeQuery(filters.typeSelection, columnName = "entity_info.type")?.let {
                """
                (
                    entity_info.type IS NULL OR
                    $it
                )
                """.trimIndent()
            }

            return listOfNotNull(idFilter, typeFilter).joinToString(" AND ")
        }

        private val rowToServiceRegistration: (Map<String, Any>) -> ServiceRegistration = { row ->
            ServiceRegistration(
                id = toUri(row["id"]),
                endpoint = toUri(row["endpoint"]),
                endpointMethod = HttpMethod.valueOf(row["endpoint_method"] as String),
                entities = DataTypes.mapper.readerForListOf(EntityInfo::class.java)
                    .readValue(toJsonString(row["entities"])),
                serviceInformation = DataTypes.deserializeAs<ServiceInformation>(
                    toJsonString(row["service_information"])
                ),
                q = row["q"] as? String,
                geoQ = row["geo_q"]?.let {
                    DataTypes.deserializeAs<UnparsedGeoQuery>(toJsonString(it))
                },
                scopeQ = row["scope_q"] as? String,
                createdAt = toZonedDateTime(row["created_at"]),
                modifiedAt = toZonedDateTime(row["modified_at"])
            )
        }
    }
}
