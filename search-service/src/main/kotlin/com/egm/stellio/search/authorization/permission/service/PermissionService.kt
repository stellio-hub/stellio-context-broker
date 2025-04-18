package com.egm.stellio.search.authorization.permission.service

import arrow.core.Either
import arrow.core.Option
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toBoolean
import com.egm.stellio.search.common.util.toInt
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PERMISSION_TERM
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.buildTypeQuery
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toStringValue
import com.egm.stellio.shared.util.toUri
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
class PermissionService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate
) {

    @Transactional
    suspend fun create(
        permission: Permission,
    ): Either<APIException, Unit> = either {
        permission.validate().bind()
        checkExistence(permission.id, true).bind()

        val insertStatement =
            """
            INSERT INTO permission(
                id,
                target_id,
                target_scope,
                target_types,
                assignee,
                action,
                created_at,
                modified_at,
                assigner
            )
            VALUES(
                :id,
                :target_id,
                :target_scope,
                :target_types,
                :assignee,
                :action,
                :created_at,
                :modified_at,
                :assigner
            )
            """.trimIndent()
        databaseClient.sql(insertStatement)
            .bind("id", permission.id)
            .bind("target_id", permission.target.id)
            .bind("target_scope", permission.target.scope)
            .bind("target_types", permission.target.types)
            .bind("assignee", permission.assignee)
            .bind("action", permission.action)
            .bind("created_at", permission.createdAt)
            .bind("modified_at", permission.modifiedAt)
            .bind("assigner", permission.assigner)
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
                FROM permission
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
                    AlreadyExistsException(Permission.alreadyExistsMessage(id)).left()
                else
                    ResourceNotFoundException(Permission.notFoundMessage(id)).left()
            }

    suspend fun getById(id: URI): Either<APIException, Permission> = either {
        checkExistence(id).bind()

        val selectStatement =
            """
            SELECT id,
                target_id,
                target_scope,
                target_types,
                assignee,
                action,
                created_at,
                modified_at,
                assigner
            FROM permission  
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .oneToResult { rowToPermission(it).bind() }
    }

    suspend fun isCreatorOf(id: URI, sub: Option<Sub>): Either<APIException, Boolean> {
        val selectStatement =
            """
            SELECT assigner
            FROM permission
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
        r2dbcEntityTemplate.delete(Permission::class.java)
            .matching(query(where("id").`is`(id)))
            .execute()
    }

    @Transactional
    suspend fun update(
        permissionId: URI,
        input: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, Unit> = either {
        checkExistence(permissionId).bind()
        if (!input.containsKey(JSONLD_TYPE_TERM) || input[JSONLD_TYPE_TERM]!! != NGSILD_PERMISSION_TERM)
            raise(BadRequestDataException("type attribute must be present and equal to '$NGSILD_PERMISSION_TERM'"))

        input.filterKeys {
            it !in JsonLdUtils.JSONLD_COMPACTED_ENTITY_CORE_MEMBERS
        }.plus("modifiedAt" to ngsiLdDateTime())
            .forEach {
                when {
                    it.key == "target" -> {
                        val target = input["target"] as Map<String, Any>
                        target["id"]?.let {
                            updatePermissionAttribute(permissionId, "target_id", it).bind()
                        }
                        target["scope"]?.let {
                            updatePermissionAttribute(permissionId, "target_scope", it).bind()
                        }
                        target["types"]?.let {// todo expand
                            updatePermissionAttribute(permissionId, "target_types", it?.map { expandJsonLdTerm(it, contexts)}).bind()
                        }
                    }

                    listOf(
                        "id",
                        "assignee",
                        "action",
                        "assigner",
                        "modifiedAt"
                    ).contains(it.key) -> {
                        val columnName = it.key
                        val value = it.value
                        updatePermissionAttribute(permissionId, columnName, value).bind()
                    }

                    else -> {
                        BadRequestDataException("invalid permission parameter  ${it.key}")
                            .left().bind<Unit>()
                    }
                }
            }
    }

    private suspend fun updatePermissionAttribute(
        permissionId: URI,
        columnName: String,
        value: Any?
    ): Either<APIException, Unit> {
        val updateStatement = Update.update(columnName, value)
        return r2dbcEntityTemplate.update(Permission::class.java)
            .matching(query(where("id").`is`(permissionId)))
            .apply(updateStatement)
            .map { Unit.right() }
            .awaitFirst()
    }

    suspend fun getPermissions(
        filters: CSRFilters = CSRFilters(),
        limit: Int = Int.MAX_VALUE,
        offset: Int = 0,
    ): Either<APIException, List<Permission>> = either {
        val filterQuery = buildWhereStatement(filters)

        val selectStatement =
            """
            SELECT id,
                target_id,
                target_scope,
                target_types,
                assignee,
                action,
                created_at,
                modified_at,
                assigner
            FROM permission
            WHERE $filterQuery
            ORDER BY permission.created_at
            LIMIT :limit
            OFFSET :offset
            """.trimIndent()

        databaseClient.sql(selectStatement)
            .bind("limit", limit)
            .bind("offset", offset)
            .allToMappedList { rowToPermission(it).bind() }
    }

    suspend fun getPermissionsCount(
        filters: CSRFilters = CSRFilters(),
    ): Either<APIException, Int> {
        val filterQuery = buildWhereStatement(filters)

        val selectStatement =
            """
            SELECT count(distinct csr.id)
            FROM permission as csr
            LEFT JOIN jsonb_to_recordset(information)
                as information(entities jsonb, "propertyNames" text[], "relationshipNames" text[]) on true
            LEFT JOIN jsonb_to_recordset(entities)
                as entity_info(id text, "idPattern" text, type text[]) on true
            WHERE $filterQuery
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .oneToResult { toInt(it["count"]) }
    }

    companion object {

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

            val filters = listOfNotNull(idFilter, typeFilter, idPatternFilter)

            return if (filters.isEmpty()) "true" else filters.joinToString(" AND ")
        }

        private fun rowToPermission(row: Map<String, Any>): Either<APIException, Permission> = either {
            Permission(
                id = toUri(row["id"]),
                createdAt = toZonedDateTime(row["created_at"]),
                modifiedAt = toZonedDateTime(row["modified_at"]),
                target = TargetAsset(
                    id = (row["target_id"] as? String)?.toUri(),
                    scope = row["target_scope"] as? String,
                    types = (row["target_types"] as? Array<String>)?.toList(),
                ),
                action = Action.fromString(row["action"] as String).bind(),
                assigner = row["assigner"] as String,
            )
        }
    }
}
