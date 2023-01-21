package com.egm.stellio.search.authorization

import arrow.core.Either
import arrow.core.Option
import arrow.core.Some
import arrow.core.getOrElse
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.SubjectType
import io.r2dbc.postgresql.codec.Json
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class SubjectReferentialService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
) {

    @Transactional
    suspend fun create(subjectReferential: SubjectReferential): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                INSERT INTO subject_referential
                    (subject_id, subject_type, subject_info, service_account_id, global_roles, 
                        groups_memberships)
                    VALUES (:subject_id, :subject_type, :subject_info, :service_account_id, :global_roles, 
                        :groups_memberships)
                ON CONFLICT (subject_id)
                    DO UPDATE SET service_account_id = :service_account_id,
                        global_roles = :global_roles,
                        groups_memberships = :groups_memberships
                """.trimIndent()
            )
            .bind("subject_id", subjectReferential.subjectId)
            .bind("subject_type", subjectReferential.subjectType.toString())
            .bind("subject_info", Json.of(subjectReferential.subjectInfo))
            .bind("service_account_id", subjectReferential.serviceAccountId)
            .bind("global_roles", subjectReferential.globalRoles?.map { it.key }?.toTypedArray())
            .bind("groups_memberships", subjectReferential.groupsMemberships?.toTypedArray())
            .execute()

    suspend fun retrieve(sub: Sub): Either<APIException, SubjectReferential> =
        databaseClient
            .sql(
                """
                SELECT *
                FROM subject_referential
                WHERE subject_id = :subject_id                
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .oneToResult(AccessDeniedException("No subject information found for $sub")) {
                rowToSubjectReferential(it)
            }

    suspend fun getSubjectAndGroupsUUID(sub: Option<Sub>): Either<APIException, List<Sub>> =
        databaseClient
            .sql(
                """
                SELECT subject_id, service_account_id, groups_memberships
                FROM subject_referential
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", (sub as Some).value)
            .oneToResult(AccessDeniedException("No subject information found for ${sub.value}")) {
                val subs = (toOptionalList<Sub>(it["groups_memberships"]) ?: emptyList())
                    .plus(it["subject_id"] as Sub)
                if (it["service_account_id"] != null)
                    subs.plus(it["service_account_id"] as Sub)
                else subs
            }

    suspend fun getGroups(sub: Option<Sub>, offset: Int, limit: Int): List<Group> =
        databaseClient
            .sql(
                """
                WITH groups_memberships AS (
                    SELECT unnest(groups_memberships) as groups
                    FROM subject_referential
                    WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                )
                SELECT subject_id AS group_id, (subject_info->'value'->>'name') AS name
                FROM subject_referential
                WHERE subject_id IN (SELECT groups FROM groups_memberships)
                ORDER BY name
                LIMIT :limit
                OFFSET :offset
                """.trimIndent()
            )
            .bind("subject_id", (sub as Some).value)
            .bind("limit", limit)
            .bind("offset", offset)
            .allToMappedList {
                Group(
                    id = it["group_id"] as String,
                    name = it["name"] as String,
                    isMember = true
                )
            }

    suspend fun getCountGroups(sub: Option<Sub>): Either<APIException, Int> =
        databaseClient
            .sql(
                """
                SELECT count(sr.g_m) as count
                FROM subject_referential
                CROSS JOIN LATERAL unnest(subject_referential.groups_memberships) as sr(g_m)
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", (sub as Some).value)
            .oneToResult { toInt(it["count"]) }

    suspend fun getAllGroups(sub: Option<Sub>, offset: Int, limit: Int): List<Group> =
        databaseClient
            .sql(
                """
                WITH groups_memberships AS (
                    SELECT distinct(unnest(groups_memberships)) as groups
                    FROM subject_referential 
                    WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                )
                SELECT subject_id AS group_id, (subject_info->'value'->>'name') AS name,
                    (subject_id IN (SELECT groups FROM groups_memberships)) AS is_member
                FROM subject_referential
                WHERE subject_type = '${SubjectType.GROUP.name}'
                ORDER BY name
                LIMIT :limit
                OFFSET :offset
                """.trimIndent()
            )
            .bind("subject_id", (sub as Some).value)
            .bind("limit", limit)
            .bind("offset", offset)
            .allToMappedList {
                Group(
                    id = it["group_id"] as String,
                    name = it["name"] as String,
                    isMember = toBoolean(it["is_member"])
                )
            }

    suspend fun getCountAllGroups(): Either<APIException, Int> =
        databaseClient
            .sql(
                """
                SELECT count(*) as count
                FROM subject_referential
                WHERE subject_type = '${SubjectType.GROUP.name}'
                """.trimIndent()
            )
            .oneToResult { toInt(it["count"]) }

    suspend fun hasStellioAdminRole(sub: Option<Sub>): Either<APIException, Boolean> =
        hasStellioRole(sub, GlobalRole.STELLIO_ADMIN)

    suspend fun hasStellioRole(sub: Option<Sub>, globalRole: GlobalRole): Either<APIException, Boolean> =
        databaseClient
            .sql(
                """
                SELECT COUNT(subject_id) as count
                FROM subject_referential
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                AND '${globalRole.key}' = ANY(global_roles)
                """.trimIndent()
            )
            .bind("subject_id", (sub as Some).value)
            .oneToResult { it["count"] as Long == 1L }

    suspend fun getGlobalRoles(sub: Option<Sub>): List<Option<GlobalRole>> =
        databaseClient
            .sql(
                """
                SELECT unnest(global_roles) as global_roles
                FROM subject_referential
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", (sub as Some).value)
            .allToMappedList { GlobalRole.forKey(it["global_roles"] as String) }

    @Transactional
    suspend fun setGlobalRoles(sub: Sub, newRoles: List<GlobalRole>): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET global_roles = :global_roles
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .bind("global_roles", newRoles.map { it.key }.toTypedArray())
            .execute()

    @Transactional
    suspend fun resetGlobalRoles(sub: Sub): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET global_roles = null
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .execute()

    @Transactional
    suspend fun addGroupMembershipToUser(sub: Sub, groupId: Sub): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET groups_memberships = array_append(groups_memberships, :group_id::text)
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .bind("group_id", groupId)
            .execute()

    @Transactional
    suspend fun removeGroupMembershipToUser(sub: Sub, groupId: Sub): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET groups_memberships = array_remove(groups_memberships, :group_id::text)
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .bind("group_id", groupId)
            .execute()

    @Transactional
    suspend fun addServiceAccountIdToClient(subjectId: Sub, serviceAccountId: Sub): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET service_account_id = :service_account_id
                WHERE subject_id = :subject_id
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .bind("service_account_id", serviceAccountId)
            .execute()

    @Transactional
    suspend fun updateSubjectInfo(subjectId: Sub, newSubjectInfo: Pair<String, String>): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET subject_info = jsonb_set(
                    subject_info,
                    '{value,${newSubjectInfo.first}}',
                    '"${newSubjectInfo.second}"',
                    true
                )
                WHERE subject_id = :subject_id
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .execute()

    @Transactional
    suspend fun delete(sub: Sub): Either<APIException, Unit> =
        r2dbcEntityTemplate.delete(SubjectReferential::class.java)
            .matching(Query.query(Criteria.where("subject_id").`is`(sub)))
            .execute()

    private fun rowToSubjectReferential(row: Map<String, Any>) =
        SubjectReferential(
            subjectId = row["subject_id"] as Sub,
            subjectType = SubjectType.valueOf(row["subject_type"] as String),
            subjectInfo = toJsonString(row["subject_info"]),
            serviceAccountId = row["service_account_id"] as? Sub,
            globalRoles = toOptionalList<String>(row["global_roles"])
                ?.mapNotNull { GlobalRole.forKey(it).getOrElse { null } },
            groupsMemberships = toOptionalList(row["groups_memberships"])
        )
}
