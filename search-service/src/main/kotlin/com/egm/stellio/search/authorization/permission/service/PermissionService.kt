package com.egm.stellio.search.authorization.permission.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.PermissionFilters
import com.egm.stellio.search.authorization.permission.model.PermissionFilters.Companion.OnlyGetPermission
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toBoolean
import com.egm.stellio.search.common.util.toInt
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.AuthContextModel.AUTH_ACTION_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_ASSIGNEE_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_ASSIGNER_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PERMISSION_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TARGET_TERM
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.buildTypeQuery
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Component
class PermissionService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val applicationProperties: ApplicationProperties,
    private val subjectReferentialService: SubjectReferentialService,
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
                assignee,
                action,
                created_at,
                modified_at,
                assigner
            )
            VALUES(
                :id,
                :target_id,
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
            .bind("assignee", permission.assignee)
            .bind("action", permission.action.value)
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

    suspend fun isAdminOf(entityId: URI): Either<APIException, Boolean> = either {
        val selectStatement =
            """
            SELECT target_id
            FROM permission
            WHERE id = :entity_id
            """.trimIndent()

        val targetEntityId = databaseClient.sql(selectStatement)
            .bind("entity_id", entityId)
            .oneToResult { it["target_id"] as String }.bind()
        checkHasPermissionOnEntity(targetEntityId.toUri(), Action.ADMIN).bind()
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
        contexts: List<String>,
    ): Either<APIException, Unit> = either {
        checkExistence(permissionId).bind()
        val sub = getSubFromSecurityContext()
        if (!input.containsKey(NGSILD_TYPE_TERM) || input[NGSILD_TYPE_TERM]!! != AUTH_PERMISSION_TERM)
            raise(BadRequestDataException("type attribute must be present and equal to '$AUTH_PERMISSION_TERM'"))

        input.filterKeys {
            it !in COMPACTED_ENTITY_CORE_MEMBERS + AUTH_ASSIGNER_TERM
        }.plus(NGSILD_MODIFIED_AT_TERM to ngsiLdDateTime()).plus(AUTH_ASSIGNER_TERM to sub.orEmpty())
            .forEach {
                when {
                    it.key == AUTH_TARGET_TERM -> {
                        val target = input[AUTH_TARGET_TERM] as Map<String, Any>
                        target[NGSILD_ID_TERM]?.let { entityId ->
                            updatePermissionAttribute(permissionId, "target_id", entityId).bind()
                        }
                    }

                    it.key == AUTH_ACTION_TERM -> {
                        updatePermissionAttribute(
                            permissionId,
                            it.key,
                            it.value as String
                        ).bind()
                    }

                    it.key == NGSILD_MODIFIED_AT_TERM -> {
                        updatePermissionAttribute(
                            permissionId,
                            "modified_at",
                            it.value
                        ).bind()
                    }
                    listOf(
                        NGSILD_ID_TERM,
                        AUTH_ASSIGNEE_TERM,
                        AUTH_ASSIGNER_TERM,
                    ).contains(it.key) -> {
                        val columnName = it.key
                        val value = it.value
                        updatePermissionAttribute(permissionId, columnName, value).bind()
                    }

                    else -> {
                        BadRequestDataException("invalid permission parameter  ${it.key}")
                            .left().bind()
                    }
                }
            }
    }

    private suspend fun updatePermissionAttribute(
        permissionId: URI,
        columnName: String,
        value: Any?
    ): Either<APIException, Unit> {
        val updateStatement =
            """
            UPDATE permission
            SET $columnName = '$value'
            WHERE permission.id = '$permissionId'
            """.trimIndent()

        return databaseClient.sql(updateStatement)
            .execute()
    }

    suspend fun getPermissions(
        filters: PermissionFilters = PermissionFilters(),
        limit: Int = Int.MAX_VALUE,
        offset: Int = 0,
    ): Either<APIException, List<Permission>> = either {
        val filterQuery = buildWhereStatement(filters).bind()
        val authorizationFilter = buildAuthorizationFilter(filters.onlyGetPermission).bind()
        val selectStatement =
            """
            SELECT id,
                target_id,
                assignee,
                action,
                created_at,
                modified_at,
                assigner
            FROM permission
            WHERE $filterQuery 
            AND $authorizationFilter
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
        filters: PermissionFilters = PermissionFilters(),
    ): Either<APIException, Int> = either {
        val filterQuery = buildWhereStatement(filters).bind()
        val authorizationFilter = buildAuthorizationFilter(filters.onlyGetPermission).bind()

        val selectStatement =
            """
            SELECT count(distinct permission.id)
            FROM permission
            WHERE $filterQuery 
            AND $authorizationFilter
            """.trimIndent()

        databaseClient.sql(selectStatement)
            .oneToResult { toInt(it["count"]) }.bind()
    }

    @Transactional
    suspend fun removePermissionsOnEntity(entityId: URI): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                DELETE FROM permission
                WHERE target_id = :entity_id
                """.trimIndent()
            )
            .bind("entity_id", entityId)
            .execute()

    internal suspend fun checkHasPermissionOnEntity(
        entityId: URI,
        action: Action
    ): Either<APIException, Boolean> = either {
        if (!applicationProperties.authentication.enabled)
            return@either true

        val subjectUuids = subjectReferentialService.getSubjectAndGroupsUUID().bind()

        subjectReferentialService.hasStellioAdminRole(subjectUuids)
            .flatMap {
                if (!it)
                    hasPermissionOnEntity(subjectUuids, entityId, action.getIncludedIn())
                else true.right()
            }.bind()
    }

    private suspend fun hasPermissionOnEntity(
        uuids: List<Sub>,
        entityId: URI,
        actions: Set<Action>
    ): Either<APIException, Boolean> =
        databaseClient
            .sql(
                """
                SELECT COUNT(id) as count
                FROM permission
                WHERE (assignee is null OR assignee IN(:uuids))
                AND target_id = :entity_id
                AND action IN(:actions)
                """.trimIndent()
            )
            .bind("uuids", uuids)
            .bind("entity_id", entityId)
            .bind("actions", actions.map { it.value })
            .oneToResult { it["count"] as Long >= 1L }

    suspend fun getEntitiesIdsOwnedBySubject(
        subjectId: Sub
    ): Either<APIException, List<URI>> = either {
        databaseClient
            .sql(
                """
                SELECT target_id 
                FROM permission
                WHERE assignee = :sub
                AND action = 'own'
                """.trimIndent()
            )
            .bind("sub", subjectId)
            .allToMappedList { toUri(it["target_id"]) }
    }

    private suspend fun buildAuthorizationFilter(onlyPermission: OnlyGetPermission?): Either<APIException, String> =
        either {
            val uuids = subjectReferentialService.getSubjectAndGroupsUUID().bind()
            when (onlyPermission) {
                OnlyGetPermission.ADMIN -> buildIsAdminFilter(uuids).bind()
                OnlyGetPermission.ASSIGNED -> buildIsAssigneeFilter(uuids)
                else -> buildIsAdminFilter(uuids).bind()
            }
        }

    private suspend fun buildIsAssigneeFilter(uuids: List<Sub>): String =
        """
        (
            assignee is null
            OR assignee IN (${uuids.joinToString(",") { "'$it'" }})
         )
        """.trimIndent()

    private suspend fun buildIsAdminFilter(uuids: List<Sub>): Either<APIException, String> = either {
        if (subjectReferentialService.hasStellioAdminRole(uuids).bind()) "true" else """
        (
            target_id in (
                SELECT target_id
                FROM permission
                WHERE assignee IN (${uuids.joinToString(",") { "'$it'" }})
                AND action IN ('admin', 'own')
            )
         )
        """.trimIndent()
    }

    private suspend fun buildWhereStatement(
        permissionFilters: PermissionFilters
    ): Either<APIException, String> = either {
        val idFilter = if (!permissionFilters.ids.isNullOrEmpty())
            """
                (
                    target_id is null OR
                    target_id in ('${permissionFilters.ids.joinToString("', '")}')
                )
            """.trimIndent()
        else null

        val actionFilter = permissionFilters.action?.let { action ->
            """
            action in ('${
                action.getIncludedIn().joinToString("', '") { it.value }
            }')
            """.trimIndent()
        }

        val assigneeFilter = permissionFilters.assignee?.let {
            val assignee = permissionFilters.assignee

            val assigneeUuids = subjectReferentialService.getSubjectAndGroupsUUID(assignee).bind()
            """
                (
                    assignee is null OR
                    assignee in ('${assigneeUuids.joinToString("', '")}')
                )
            """
        }

        val assignerFilter = permissionFilters.assigner?.let { assigner ->
            "assigner = '$assigner'"
        }

        val targetTypeFilter = if (!permissionFilters.targetTypeSelection.isNullOrEmpty())
            """
                (
                    target_id is null OR
                    target_id in (
                      SELECT entity_id
                      FROM entity_payload
                      where ${buildTypeQuery(permissionFilters.targetTypeSelection)})
                )
            """.trimIndent()
        else null

        val filters = listOfNotNull(idFilter, actionFilter, assigneeFilter, assignerFilter, targetTypeFilter)

        if (filters.isEmpty()) "true" else filters.joinToString(" AND ")
    }

    companion object {

        private fun rowToPermission(row: Map<String, Any>): Either<APIException, Permission> = either {
            Permission(
                id = toUri(row["id"]),
                createdAt = toZonedDateTime(row["created_at"]),
                modifiedAt = toZonedDateTime(row["modified_at"]),
                target = TargetAsset(
                    id = (row["target_id"] as String).toUri(),
                ),
                action = Action.fromString(row["action"] as String).bind(),
                assigner = row["assigner"] as? String,
                assignee = row["assignee"] as? String
            )
        }
    }
}
