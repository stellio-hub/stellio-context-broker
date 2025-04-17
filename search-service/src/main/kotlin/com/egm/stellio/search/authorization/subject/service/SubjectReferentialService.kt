package com.egm.stellio.search.authorization.subject.service

import arrow.core.Either
import arrow.core.Some
import arrow.core.getOrElse
import com.egm.stellio.search.authorization.subject.model.Group
import com.egm.stellio.search.authorization.subject.model.SubjectReferential
import com.egm.stellio.search.authorization.subject.model.User
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toBoolean
import com.egm.stellio.search.common.util.toInt
import com.egm.stellio.search.common.util.toJson
import com.egm.stellio.search.common.util.toJsonString
import com.egm.stellio.search.common.util.toOptionalList
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import com.egm.stellio.shared.util.ADMIN_ROLES
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.getSubFromSecurityContext
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class SubjectReferentialService(
    private val databaseClient: DatabaseClient
) {

    @Transactional
    suspend fun create(subjectReferential: SubjectReferential): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                INSERT INTO subject_referential
                    (subject_id, subject_type, subject_info, global_roles, groups_memberships)
                    VALUES (:subject_id, :subject_type, :subject_info, :global_roles, :groups_memberships)
                ON CONFLICT (subject_id)
                    DO UPDATE SET global_roles = :global_roles,
                        groups_memberships = :groups_memberships
                """.trimIndent()
            )
            .bind("subject_id", subjectReferential.subjectId)
            .bind("subject_type", subjectReferential.subjectType.toString())
            .bind("subject_info", subjectReferential.subjectInfo)
            .bind("global_roles", subjectReferential.globalRoles?.map { it.key }?.toTypedArray())
            .bind("groups_memberships", subjectReferential.groupsMemberships?.toTypedArray())
            .execute()

    @Transactional
    suspend fun upsertClient(subjectReferential: SubjectReferential): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                INSERT INTO subject_referential
                    (subject_id, subject_type, subject_info)
                    VALUES (:subject_id, :subject_type, :subject_info)
                ON CONFLICT (subject_id)
                    DO UPDATE SET subject_info = :subject_info
                """.trimIndent()
            )
            .bind("subject_id", subjectReferential.subjectId)
            .bind("subject_type", subjectReferential.subjectType.toString())
            .bind("subject_info", subjectReferential.subjectInfo)
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

    suspend fun getSubjectAndGroupsUUID(): Either<APIException, List<Sub>> =
        databaseClient
            .sql(
                """
                SELECT subject_id, groups_memberships
                FROM subject_referential
                WHERE subject_id = :subject_id
                """.trimIndent()
            )
            .bind("subject_id", (getSubFromSecurityContext() as Some).value)
            .oneToResult(
                AccessDeniedException(
                    "No subject information found for ${(getSubFromSecurityContext() as Some).value}"
                )
            ) {
                toOptionalList<Sub>(it["groups_memberships"]).orEmpty()
                    .plus(it["subject_id"] as Sub)
            }

    suspend fun getGroups(offset: Int, limit: Int): List<Group> =
        databaseClient
            .sql(
                """
                WITH groups_memberships AS (
                    SELECT unnest(groups_memberships) as groups
                    FROM subject_referential
                    WHERE subject_id = :subject_id
                )
                SELECT subject_id AS group_id, (subject_info->'value'->>'name') AS name
                FROM subject_referential
                WHERE subject_id IN (SELECT groups FROM groups_memberships)
                ORDER BY name
                LIMIT :limit
                OFFSET :offset
                """.trimIndent()
            )
            .bind("subject_id", (getSubFromSecurityContext() as Some).value)
            .bind("limit", limit)
            .bind("offset", offset)
            .allToMappedList {
                Group(
                    id = it["group_id"] as String,
                    name = it["name"] as String,
                    isMember = true
                )
            }

    suspend fun getCountGroups(): Either<APIException, Int> =
        databaseClient
            .sql(
                """
                SELECT count(sr.g_m) as count
                FROM subject_referential
                CROSS JOIN LATERAL unnest(subject_referential.groups_memberships) as sr(g_m)
                WHERE subject_id = :subject_id
                """.trimIndent()
            )
            .bind("subject_id", (getSubFromSecurityContext() as Some).value)
            .oneToResult { toInt(it["count"]) }

    suspend fun getAllGroups(offset: Int, limit: Int): List<Group> =
        databaseClient
            .sql(
                """
                WITH groups_memberships AS (
                    SELECT distinct(unnest(groups_memberships)) as groups
                    FROM subject_referential 
                    WHERE subject_id = :subject_id
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
            .bind("subject_id", (getSubFromSecurityContext() as Some).value)
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

    suspend fun getUsers(offset: Int, limit: Int): List<User> =
        databaseClient
            .sql(
                """
                SELECT subject_id AS user_id, (subject_info->'value'->>'username') AS username,
                    (subject_info->'value'->>'givenName') AS givenName,
                    (subject_info->'value'->>'familyName') AS familyName,
                    subject_info
                FROM subject_referential
                WHERE subject_type = '${SubjectType.USER.name}'
                ORDER BY username
                LIMIT :limit
                OFFSET :offset
                """.trimIndent()
            )
            .bind("limit", limit)
            .bind("offset", offset)
            .allToMappedList {
                User(
                    id = it["user_id"] as String,
                    username = it["username"] as String,
                    givenName = it["givenName"] as? String,
                    familyName = it["familyName"] as? String,
                    subjectInfo = toJsonString(it["subject_info"])
                        .deserializeAsMap()[NGSILD_VALUE_TERM] as Map<String, String>
                )
            }

    suspend fun getUsersCount(): Either<APIException, Int> =
        databaseClient
            .sql(
                """
                SELECT count(*) as count
                FROM subject_referential
                WHERE subject_type = '${SubjectType.USER.name}'
                """.trimIndent()
            )
            .oneToResult { toInt(it["count"]) }

    suspend fun hasStellioAdminRole(uuids: List<Sub>): Either<APIException, Boolean> =
        hasOneOfGlobalRoles(uuids, ADMIN_ROLES)

    suspend fun hasOneOfGlobalRoles(uuids: List<Sub>, roles: Set<GlobalRole>): Either<APIException, Boolean> =
        databaseClient
            .sql(
                """
                SELECT COUNT(subject_id) as count
                FROM subject_referential
                WHERE subject_id IN(:uuids)
                AND global_roles && :roles
                """.trimIndent()
            )
            .bind("uuids", uuids)
            .bind("roles", roles.map { it.key }.toTypedArray())
            .oneToResult { it["count"] as Long >= 1L }

    @Transactional
    suspend fun setGlobalRoles(sub: Sub, newRoles: List<GlobalRole>): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET global_roles = :global_roles
                WHERE subject_id = :subject_id
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
                WHERE subject_id = :subject_id
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
                WHERE subject_id = :subject_id
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
                WHERE subject_id = :subject_id
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .bind("group_id", groupId)
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
        databaseClient
            .sql(
                """
                DELETE FROM subject_referential
                WHERE subject_id = :subject_id
                OR jsonb_path_match(subject_info, 'exists($.value.internalClientId ? (@ == ${'$'}value))', '{ "value": "$sub" }')
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .execute()

    private fun rowToSubjectReferential(row: Map<String, Any>) =
        SubjectReferential(
            subjectId = row["subject_id"] as Sub,
            subjectType = SubjectType.valueOf(row["subject_type"] as String),
            subjectInfo = toJson(row["subject_info"]),
            globalRoles = toOptionalList<String>(row["global_roles"])
                ?.mapNotNull { GlobalRole.forKey(it).getOrElse { null } },
            groupsMemberships = toOptionalList(row["groups_memberships"])
        )
}
