package com.egm.stellio.search.authorization.permission.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.alreadyCoveredMessage
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
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.SeeOtherException
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.buildScopeQQuery
import com.egm.stellio.shared.util.buildTypeQuery
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
        checkExistence(permission.id, true).bind()
        checkDuplicate(permission).bind()
        upsert(permission).bind()
    }

    @Transactional
    suspend fun upsert(
        permission: Permission,
    ): Either<APIException, Unit> = either {
        permission.validate().bind()

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
            ON CONFLICT (id) 
                DO UPDATE SET target_id = :target_id,
                    target_scopes = :target_scopes,
                    target_types = :target_types,
                    assignee = :assignee,
                    action = :action,
                    created_at = :created_at,
                    modified_at = :modified_at,
                    assigner = :assigner
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
        val targetIdIsIncludedFilter = permission.target.id?.let { " target_id = '$it'" } ?: "target_id is null"
        val targetTypesAreIncludedFilter =
            permission.target.types?.let { "(target_types is null OR target_types @> ${it.toSqlArray()})" }
                ?: "target_types is null"
        val targetScopesAreIncludedFilter =
            permission.target.scopes?.let { "(target_scopes is null OR target_scopes @> ${it.toSqlArray()})" }
                ?: "target_scopes is null"

        val targetIsIncludedFilter =
            listOf(targetIdIsIncludedFilter, targetTypesAreIncludedFilter, targetScopesAreIncludedFilter)
                .joinToString(" AND ")

        return databaseClient.sql(
            """
                SELECT id
                FROM permission
                WHERE action = :action
                AND $targetIsIncludedFilter
                AND assignee = '${permission.assignee}'                  
            """.trimIndent()
        )
            .bind("action", permission.action.value)
            .allToMappedList { toUri(it["id"]) }
            .let {
                if (it.isNotEmpty()) {
                    val duplicateId = it.first()
                    SeeOtherException(alreadyCoveredMessage(duplicateId), duplicateId).left()
                } else
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

    suspend fun getPermissions(
        filters: PermissionFilters = PermissionFilters(),
        limit: Int = Int.MAX_VALUE,
        offset: Int = 0,
    ): Either<APIException, List<Permission>> = either {
        // add a filter that only return permission matching the received PermissionFilter
        val filterQuery = buildPermissionFiltersWhereStatement(filters).bind()
        // add a filter that only return permission the current subject administrate
        val (withClause, authorizationFilter) = buildPermissionAuthorizationFilter(filters.kind).bind()
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
        val (withClause, authorizationFilter) = buildPermissionAuthorizationFilter(filters.kind).bind()

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

    internal suspend fun checkHasPermissionOnEntity(
        entityId: URI,
        action: Action
    ): Either<APIException, Boolean> = either {
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

    fun buildAsRightOnEntityFilter(
        action: Action,
        uuids: List<Sub>
    ): String = """
        (
            -- You have the right on an entity
            -- if you have a permission targeting the entity id
            entity_payload.entity_id in (
              SELECT target_id
              FROM permission
              WHERE ${buildIsAssigneeFilter(uuids)}
              AND target_id IS NOT NULL
              AND action IN ${action.includedInToSqlList()}
           ) 
           OR 
           -- if you have a permission on a type and a scope of the entity
           exists (
               SELECT 1
               FROM candidate_permissions as cp
               WHERE (cp.target_types is null OR cp.target_types && types)
               AND (cp.target_scopes is null OR cp.target_scopes && COALESCE(scopes, ARRAY['@none']))
           )
        )
    """.trimIndent()

    internal suspend fun hasPermissionOnTarget(
        target: TargetAsset,
        action: Action,
    ): Either<APIException, Unit> = either {
        if (!applicationProperties.authentication.enabled)
            return Unit.right()

        val subjectUuids = subjectReferentialService.getSubjectAndGroupsUUID().bind()

        subjectReferentialService.hasStellioAdminRole(subjectUuids)
            .flatMap {
                if (!it && !subjectsHavePermissionsOnTarget(subjectUuids, target, action.getIncludedIn()).bind())
                    AccessDeniedException(unauthorizedTargetMessage(target)).left().bind()
                else Unit.right()
            }.bind()
    }

    private suspend fun subjectsHavePermissionsOnTarget(
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

    private suspend fun buildPermissionAuthorizationFilter(
        kind: PermissionKind = PermissionKind.ADMIN
    ): Either<APIException, WithAndFilter> = either {
        val uuids = subjectReferentialService.getSubjectAndGroupsUUID().bind()
        when (kind) {
            // you can fetch the permission assigned to you
            PermissionKind.ASSIGNED -> "" to buildIsAssigneeFilter(uuids)
            // or you can fetch the permission you administrate
            PermissionKind.ADMIN -> buildAdministratedPermissionFilter(uuids).bind()
        }
    }

    private fun buildIsAssigneeFilter(uuids: List<Sub>): String =
        "(assignee IN ${uuids.toSqlList()})"

    private suspend fun buildAdministratedPermissionFilter(uuids: List<Sub>): Either<APIException, WithAndFilter> =
        either {
            if (subjectReferentialService.hasStellioAdminRole(uuids).bind())
                "" to "true"
            else {
                val withClause = buildCandidatePermissionsWithStatement(Action.ADMIN, uuids)
                val filterClause = """
                (
                    -- if the permission target an entity, you need to admin this entity
                    (
                        target_id is not null 
                        AND ${buildAsRightOnEntityFilter(Action.ADMIN, uuids)}
                    )   
                    OR
                    -- if the permission target types and scopes, you need to administrate this types and scopes
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

    fun buildCandidatePermissionsWithStatement(
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
                -- target type selection filter
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
                -- target scope selection filter
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
                -- targetId filter
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
                assigner = row["assigner"] as String,
                assignee = row["assignee"] as String
            )
        }
    }
}
