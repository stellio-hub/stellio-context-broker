package com.egm.stellio.search.authorization.permission.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.UNIQUENESS_CONFLICT_MESSAGE
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.unauthorizedTargetMessage
import com.egm.stellio.search.authorization.permission.model.PermissionFilters
import com.egm.stellio.search.authorization.permission.model.PermissionFilters.Companion.PermissionKind
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toBoolean
import com.egm.stellio.search.common.util.toInt
import com.egm.stellio.search.common.util.toOptionalList
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
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
import com.egm.stellio.shared.util.buildScopeQQuery
import com.egm.stellio.shared.util.buildTypeQuery
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toSqlArray
import com.egm.stellio.shared.util.toSqlList
import com.egm.stellio.shared.util.toUri
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

typealias WithAndFilter = Pair<String, String>

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
        checkDuplicate(permission).bind()

        val insertStatement =
            """
            INSERT INTO permission(
                id,
                target_id,
                target_scopes,
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
                :target_scopes,
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
            .bind("target_scopes", permission.target.scopes?.toTypedArray())
            .bind("target_types", permission.target.types?.toTypedArray())
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

    suspend fun checkDuplicate(
        permission: Permission
    ): Either<APIException, Unit> {
        val targetIdFilter = permission.target.id?.let { " target_id = '$it'" } ?: "target_id is null"
        val scopesIsIncludedFilter =
            permission.target.scopes?.let { "(target_scopes is null OR target_scopes @> ${it.toSqlArray()})" }
                ?: "target_scopes is null"
        val typesIsIncludedFilter =
            permission.target.types?.let { "(target_types is null OR target_types @> ${it.toSqlArray()})" }
                ?: "target_types is null"

        val targetIsIncludedFilter =
            listOf(targetIdFilter, scopesIsIncludedFilter, typesIsIncludedFilter)
                .joinToString(" AND ")
        return databaseClient.sql(
            """
            SELECT exists (
                SELECT 1
                FROM permission
                WHERE action = :action
                AND $targetIsIncludedFilter
                AND assignee ${if (permission.assignee.isNullOrBlank()) "is null" else "= '${permission.assignee}'"}                   
            ) as exists
            """.trimIndent()
        )
            .bind("action", permission.action.value)
            .oneToResult { toBoolean(it["exists"]) }
            .flatMap {
                if (it)
                    AlreadyExistsException(UNIQUENESS_CONFLICT_MESSAGE).left()
                else
                    Unit.right()
            }
    }

    suspend fun getById(id: URI): Either<APIException, Permission> = either {
        checkExistence(id).bind()

        val selectStatement =
            """
            SELECT id,
                target_id,
                target_scopes,
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
                        val target = TargetAsset.deserialize(it.value as Map<String, Any>, contexts).bind()
                        updatePermissionAttribute(permissionId, "target_id", target.id).bind()
                        updatePermissionAttribute(
                            permissionId,
                            "target_types",
                            target.types?.toSqlArray()
                        ).bind()
                        updatePermissionAttribute(
                            permissionId,
                            "target_scopes",
                            target.scopes?.toSqlArray()
                        ).bind()
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
        val filterQuery = buildPermissionFiltersWhereStatement(filters).bind()
        val (withClause, authorizationFilter) = buildAuthorizationFilter(filters.kind).bind()
        val selectStatement =
            """
            $withClause
                
            SELECT 
                permission.id,
                permission.target_id,
                permission.target_scopes,
                permission.target_types,
                permission.assignee,
                permission.action,
                permission.created_at,
                permission.modified_at,
                permission.assigner
            FROM permission
            LEFT JOIN entity_payload ON permission.target_id = entity_payload.entity_id
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
        val filterQuery = buildPermissionFiltersWhereStatement(filters).bind()
        val (withClause, authorizationFilter) = buildAuthorizationFilter(filters.kind).bind()

        val selectStatement =
            """
            $withClause
            
            SELECT count(distinct permission.id)
            FROM permission
            LEFT JOIN entity_payload ON permission.target_id = entity_payload.entity_id
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
            return true.right()

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
    ): Either<APIException, Boolean> = databaseClient
        .sql(
            """
                WITH entity AS (
                    SELECT
                        entity_id,
                        types,
                        COALESCE(scopes, ARRAY['@none']) as scopes
                    FROM entity_payload
                    WHERE entity_id = '$entityId'                
                )
                
                SELECT COUNT(id) as count
                FROM permission
                LEFT JOIN entity ON TRUE
                WHERE ${buildIsAssigneeFilter(uuids)}
                AND (
                    target_id = '$entityId'
                    OR (
                        permission.target_id is null 
                        AND
                        (permission.target_scopes is null OR permission.target_scopes && entity.scopes)
                        AND
                        (permission.target_types is null OR permission.target_types && entity.types)
                    )
                )
                AND action IN(:actions)
            """.trimIndent()
        )
        .bind("actions", actions.map { it.value })
        .oneToResult { it["count"] as Long >= 1L }

    internal suspend fun hasPermissionOnTarget(
        target: TargetAsset,
        action: Action,
    ): Either<APIException, Unit> = either {
        if (!applicationProperties.authentication.enabled)
            return Unit.right()

        val subjectUuids = subjectReferentialService.getSubjectAndGroupsUUID().bind()

        subjectReferentialService.hasStellioAdminRole(subjectUuids)
            .flatMap {
                if (!it && !hasPermissionOnTarget(subjectUuids, target, action.getIncludedIn()).bind())
                    AccessDeniedException(unauthorizedTargetMessage(target)).left().bind()
                else Unit.right()
            }.bind()
    }

    private suspend fun hasPermissionOnTarget(
        uuids: List<Sub>,
        target: TargetAsset,
        actions: Set<Action>
    ): Either<APIException, Boolean> {
        if (target.isTargetingEntity())
            return hasPermissionOnEntity(
                uuids,
                target.id!!,
                actions
            )
        val typesFilters = target.types?.let {
            "target_types @> ${target.types.toSqlArray()}"
        }
        val scopesFilters = target.scopes?.let {
            "target_scopes @> ${target.scopes.toSqlArray()}"
        }
        val scopesAndTypesFilters = listOfNotNull(typesFilters, scopesFilters).joinToString(" AND ")

        return databaseClient
            .sql(
                """
                SELECT COUNT(id) as count
                FROM permission
                LEFT JOIN entity_payload ON permission.target_id = entity_payload.entity_id
                WHERE ${buildIsAssigneeFilter(uuids)}
                AND $scopesAndTypesFilters
                AND action IN(:actions)
                """.trimIndent()
            )
            .bind("actions", actions.map { it.value })
            .oneToResult { it["count"] as Long >= 1L }
    }

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

    private suspend fun buildAuthorizationFilter(
        kind: PermissionKind = PermissionKind.ADMIN
    ): Either<APIException, WithAndFilter> =
        either {
            val uuids = subjectReferentialService.getSubjectAndGroupsUUID().bind()
            when (kind) {
                PermissionKind.ADMIN -> buildPermissionAdminFilter(uuids).bind()
                PermissionKind.ASSIGNED -> "" to buildIsAssigneeFilter(uuids)
            }
        }

    private suspend fun buildPermissionAdminFilter(uuids: List<Sub>): Either<APIException, WithAndFilter> = either {
        if (subjectReferentialService.hasStellioAdminRole(uuids).bind())
            "" to "true"
        else {
            val withClause = buildCandidatePermissionWithStatement(Action.ADMIN, uuids)
            val filterClause = """
        (
            (
                target_id is not null 
                AND ${buildAsRightOnEntityFilter(Action.ADMIN, uuids)}
            )   
            OR 
            (
               target_id is null  
               AND  exists (
                   SELECT 1
                   FROM candidate_permissions as cp
                   WHERE (cp.target_types IS NULL OR cp.target_types @> permission.target_types)
                   AND (cp.target_scopes IS NULL OR cp.target_scopes @> permission.target_scopes)
               )
            )
        )
            """.trimIndent()
            withClause to filterClause
        }
    }

    fun buildAsRightOnEntityFilter(
        action: Action,
        uuids: List<Sub>
    ): String = """
        (
            entity_payload.entity_id in (
              SELECT target_id
              FROM permission
              WHERE ${buildIsAssigneeFilter(uuids)}
              AND target_id IS NOT NULL
              AND action IN ${action.includedInToSqlList()}
           ) 
           OR exists (
               SELECT 1
               FROM candidate_permissions as cp
               WHERE (cp.target_types is null OR cp.target_types && types)
               AND (cp.target_scopes is null OR cp.target_scopes && COALESCE(scopes, ARRAY['@none']))
           )
        )
    """.trimIndent()

    fun buildCandidatePermissionWithStatement(
        action: Action,
        uuids: List<Sub>
    ): String = """
                 WITH candidate_permissions AS (
                    SELECT
                        target_types,
                        target_scopes
                    FROM permission
                    WHERE ${buildIsAssigneeFilter(uuids)}
                    AND target_id IS NULL
                    AND action IN ${action.includedInToSqlList()}
                )
    """.trimIndent()

    private suspend fun buildPermissionFiltersWhereStatement(
        permissionFilters: PermissionFilters
    ): Either<APIException, String> = either {
        val actionFilter = permissionFilters.action?.let { action ->
            """
            action in ${action.includedInToSqlList()}
            """.trimIndent()
        }

        val assigneeFilter = permissionFilters.assignee?.let {
            val assignee = permissionFilters.assignee

            val assigneeUuids = subjectReferentialService.getSubjectAndGroupsUUID(assignee).bind()
            buildIsAssigneeFilter(assigneeUuids)
        }

        val assignerFilter = permissionFilters.assigner?.let { assigner ->
            "assigner = '$assigner'"
        }

        //  targetTypeFilter also return permission targeting entity with this type
        val targetTypeFilter = if (!permissionFilters.targetTypeSelection.isNullOrEmpty())
            """
                (
                  (target_id is null AND target_types is null)
                  OR
                  ( ${buildTypeQuery(permissionFilters.targetTypeSelection, "target_types")} )
                  OR 
                  ( ${buildTypeQuery(permissionFilters.targetTypeSelection)} )
               ) 
            """.trimIndent()
        else null

        // targetScopeFilter also return permission targeting entity with this scope
        val targetScopeFilter = if (!permissionFilters.targetScopeSelection.isNullOrEmpty())
            """
                (
                  (target_id is null AND target_scopes is null)
                  OR
                  ( ${buildScopeQQuery(permissionFilters.targetScopeSelection, columnName = "target_scopes")} )
                  OR 
                  ( ${buildScopeQQuery(permissionFilters.targetScopeSelection)} )
                )
            """.trimIndent()
        else null

        // targetIdFilter return permission targeting type or scope who include the entity with targetId
        val idFilter = if (!permissionFilters.ids.isNullOrEmpty())
            """
                (
                  (
                      target_id is not null AND
                      target_id in ${permissionFilters.ids.toSqlList()}
                  )
                  OR
                  (
                      entity_id in ${permissionFilters.ids.toSqlList()}
                  )
                )
            """.trimIndent()
        else null

        val filters =
            listOfNotNull(idFilter, actionFilter, assigneeFilter, assignerFilter, targetTypeFilter, targetScopeFilter)

        if (filters.isEmpty()) "true" else filters.joinToString(" AND ")
    }

    private fun buildIsAssigneeFilter(uuids: List<Sub>): String =
        "(assignee is null OR assignee IN ${uuids.toSqlList()})"

    companion object {

        private fun rowToPermission(row: Map<String, Any>): Either<APIException, Permission> = either {
            Permission(
                id = toUri(row["id"]),
                createdAt = toZonedDateTime(row["created_at"]),
                modifiedAt = toZonedDateTime(row["modified_at"]),
                target = TargetAsset(
                    id = (row["target_id"] as? String)?.toUri(),
                    types = toOptionalList(row["target_types"]),
                    scopes = toOptionalList(row["target_scopes"])
                ),
                action = Action.fromString(row["action"] as String).bind(),
                assigner = row["assigner"] as? String,
                assignee = row["assignee"] as? String
            )
        }
    }
}
