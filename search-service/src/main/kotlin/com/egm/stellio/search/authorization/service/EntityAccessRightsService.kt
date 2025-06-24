package com.egm.stellio.search.authorization.service

import arrow.core.Either
import arrow.core.Option
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.model.EntityAccessRights
import com.egm.stellio.search.authorization.model.EntityAccessRights.SubjectRightInfo
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.executeExpected
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toEnum
import com.egm.stellio.search.common.util.toInt
import com.egm.stellio.search.common.util.toJsonString
import com.egm.stellio.search.common.util.toList
import com.egm.stellio.search.common.util.toOptionalEnum
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AccessRight.CAN_ADMIN
import com.egm.stellio.shared.util.AccessRight.CAN_READ
import com.egm.stellio.shared.util.AccessRight.CAN_WRITE
import com.egm.stellio.shared.util.AccessRight.IS_OWNER
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CLIENT_ID
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_NAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_USERNAME
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_COMPACT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_COMPACT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.AuthContextModel.USER_COMPACT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.USER_ENTITY_PREFIX
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.buildTypeQuery
import com.egm.stellio.shared.util.getSpecificAccessPolicy
import com.egm.stellio.shared.util.toUri
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Service
class EntityAccessRightsService(
    private val applicationProperties: ApplicationProperties,
    private val databaseClient: DatabaseClient,
    private val subjectReferentialService: SubjectReferentialService
) {
    @Transactional
    suspend fun setReadRoleOnEntity(sub: Sub, entityId: URI): Either<APIException, Unit> =
        setRoleOnEntity(sub, entityId, CAN_READ)

    @Transactional
    suspend fun setWriteRoleOnEntity(sub: Sub, entityId: URI): Either<APIException, Unit> =
        setRoleOnEntity(sub, entityId, CAN_WRITE)

    @Transactional
    suspend fun setOwnerRoleOnEntity(sub: Sub, entityId: URI): Either<APIException, Unit> =
        setRoleOnEntity(sub, entityId, IS_OWNER)

    @Transactional
    suspend fun setRoleOnEntity(sub: Sub, entityId: URI, accessRight: AccessRight): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                INSERT INTO entity_access_rights (subject_id, entity_id, access_right)
                    VALUES (:subject_id, :entity_id, :access_right)
                ON CONFLICT (subject_id, entity_id, access_right)
                    DO UPDATE SET access_right = :access_right
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .bind("entity_id", entityId)
            .bind("access_right", accessRight.attributeName)
            .execute()

    @Transactional
    suspend fun removeRoleOnEntity(sub: Sub, entityId: URI): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                DELETE FROM entity_access_rights
                WHERE entity_id = :entity_id
                AND subject_id = :subject_id
                """.trimIndent()
            )
            .bind("entity_id", entityId)
            .bind("subject_id", sub)
            .executeExpected {
                if (it == 0L)
                    ResourceNotFoundException("No right found for $sub on $entityId").left()
                else Unit.right()
            }

    @Transactional
    suspend fun removeRolesOnEntity(entityId: URI): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                DELETE FROM entity_access_rights
                WHERE entity_id = :entity_id
                """.trimIndent()
            )
            .bind("entity_id", entityId)
            .execute()

    suspend fun canReadEntity(sub: Option<Sub>, entityId: URI): Either<APIException, Unit> =
        checkHasRightOnEntity(
            sub,
            entityId,
            listOf(SpecificAccessPolicy.AUTH_READ, SpecificAccessPolicy.AUTH_WRITE),
            listOf(CAN_READ, CAN_WRITE, CAN_ADMIN, IS_OWNER)
        ).flatMap {
            if (!it)
                AccessDeniedException("User forbidden read access to entity $entityId").left()
            else Unit.right()
        }

    suspend fun canWriteEntity(sub: Option<Sub>, entityId: URI): Either<APIException, Unit> =
        checkHasRightOnEntity(
            sub,
            entityId,
            listOf(SpecificAccessPolicy.AUTH_WRITE),
            listOf(CAN_WRITE, CAN_ADMIN, IS_OWNER)
        ).flatMap {
            if (!it)
                AccessDeniedException("User forbidden write access to entity $entityId").left()
            else Unit.right()
        }

    suspend fun isOwnerOfEntity(subjectId: Sub, entityId: URI): Either<APIException, Boolean> =
        databaseClient
            .sql(
                """
                SELECT access_right
                FROM entity_access_rights
                WHERE subject_id = :sub
                AND entity_id = :entity_id
                """.trimIndent()
            )
            .bind("sub", subjectId)
            .bind("entity_id", entityId)
            .oneToResult { it["access_right"] as String == IS_OWNER.attributeName }

    internal suspend fun checkHasRightOnEntity(
        sub: Option<Sub>,
        entityId: URI,
        specificAccessPolicies: List<SpecificAccessPolicy>,
        accessRights: List<AccessRight>
    ): Either<APIException, Boolean> = either {
        if (!applicationProperties.authentication.enabled)
            return@either true

        val subjectUuids = subjectReferentialService.getSubjectAndGroupsUUID(sub).bind()

        subjectReferentialService.hasStellioAdminRole(subjectUuids)
            .flatMap {
                if (!it)
                    hasSpecificAccessPolicies(entityId, specificAccessPolicies)
                else true.right()
            }.flatMap {
                if (!it)
                    hasDirectAccessRightOnEntity(subjectUuids, entityId, accessRights)
                else true.right()
            }.bind()
    }

    suspend fun hasSpecificAccessPolicies(
        entityId: URI,
        specificAccessPolicies: List<SpecificAccessPolicy>
    ): Either<APIException, Boolean> {
        if (specificAccessPolicies.isEmpty())
            return either { false }

        return databaseClient.sql(
            """
            SELECT count(entity_id) as count
            FROM entity_payload
            WHERE entity_id = :entity_id
            AND specific_access_policy IN (:specific_access_policies)
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policies", specificAccessPolicies.map { it.toString() })
            .oneToResult { it["count"] as Long > 0 }
    }

    private suspend fun hasDirectAccessRightOnEntity(
        uuids: List<Sub>,
        entityId: URI,
        accessRights: List<AccessRight>
    ): Either<APIException, Boolean> =
        databaseClient
            .sql(
                """
                SELECT COUNT(subject_id) as count
                FROM entity_access_rights
                WHERE subject_id IN(:uuids)
                AND entity_id = :entity_id
                AND access_right IN(:access_rights)
                """.trimIndent()
            )
            .bind("uuids", uuids)
            .bind("entity_id", entityId)
            .bind("access_rights", accessRights.map { it.attributeName })
            .oneToResult { it["count"] as Long >= 1L }

    suspend fun getSubjectAccessRights(
        sub: Option<Sub>,
        accessRights: List<AccessRight>,
        entitiesQuery: EntitiesQueryFromGet,
        includeDeleted: Boolean = false
    ): Either<APIException, List<EntityAccessRights>> = either {
        val ids = entitiesQuery.ids
        val typeSelection = entitiesQuery.typeSelection
        val subjectUuids = subjectReferentialService.getSubjectAndGroupsUUID(sub).bind()
        val isStellioAdmin = subjectReferentialService.hasStellioAdminRole(subjectUuids).bind()

        databaseClient
            .sql(
                """
                SELECT ep.entity_id, ep.types, ear.access_right, ep.specific_access_policy, ep.deleted_at
                FROM entity_access_rights ear
                LEFT JOIN entity_payload ep ON ear.entity_id = ep.entity_id
                WHERE ${if (isStellioAdmin) "1 = 1" else "subject_id IN (:subject_uuids)" }
                ${if (accessRights.isNotEmpty()) " AND access_right IN (:access_rights)" else ""}
                ${if (!typeSelection.isNullOrEmpty()) " AND (${buildTypeQuery(typeSelection)})" else ""}
                ${if (ids.isNotEmpty()) " AND ear.entity_id IN (:entities_ids)" else ""}
                ${if (!includeDeleted) " AND deleted_at IS NULL" else ""}
                ORDER BY entity_id
                LIMIT :limit
                OFFSET :offset;
                """.trimIndent()
            )
            .bind("limit", entitiesQuery.paginationQuery.limit)
            .bind("offset", entitiesQuery.paginationQuery.offset)
            .let {
                if (!isStellioAdmin)
                    it.bind("subject_uuids", subjectUuids)
                else it
            }
            .let {
                if (accessRights.isNotEmpty())
                    it.bind("access_rights", accessRights.map { it.attributeName })
                else it
            }
            .let {
                if (ids.isNotEmpty())
                    it.bind("entities_ids", ids)
                else it
            }
            .allToMappedList { rowToEntityAccessControl(it, isStellioAdmin) }
            .groupBy { it.id }
            // a user may have multiple rights on a given entity (e.g., through groups memberships)
            // retain the one with the "higher" right
            .mapValues { (_, entityAccessRights) ->
                val ear = entityAccessRights.first()
                EntityAccessRights(
                    ear.id,
                    ear.types,
                    ear.isDeleted,
                    entityAccessRights.maxOf { it.right },
                    ear.specificAccessPolicy
                )
            }.values.toList()
    }

    suspend fun getSubjectAccessRightsCount(
        sub: Option<Sub>,
        accessRights: List<AccessRight>,
        type: EntityTypeSelection? = null,
        ids: Set<URI>? = null,
        includeDeleted: Boolean = false
    ): Either<APIException, Int> = either {
        val subjectUuids = subjectReferentialService.getSubjectAndGroupsUUID(sub).bind()
        val isStellioAdmin = subjectReferentialService.hasStellioAdminRole(subjectUuids).bind()

        databaseClient
            .sql(
                """
                SELECT count(*) as count
                FROM entity_access_rights ear
                LEFT JOIN entity_payload ep ON ear.entity_id = ep.entity_id
                WHERE ${if (isStellioAdmin) "1 = 1" else "subject_id IN (:subject_uuids)" }
                ${if (accessRights.isNotEmpty()) " AND access_right IN (:access_rights)" else ""}
                ${if (!type.isNullOrEmpty()) " AND (${buildTypeQuery(type)})" else ""}
                ${if (!ids.isNullOrEmpty()) " AND ear.entity_id IN (:entities_ids)" else ""}
                ${if (!includeDeleted) " AND deleted_at IS NULL" else ""}
                """.trimIndent()
            )
            .let {
                if (!isStellioAdmin)
                    it.bind("subject_uuids", subjectUuids)
                else it
            }
            .let {
                if (accessRights.isNotEmpty())
                    it.bind("access_rights", accessRights.map { it.attributeName })
                else it
            }
            .let {
                if (!ids.isNullOrEmpty())
                    it.bind("entities_ids", ids)
                else it
            }
            .oneToResult { toInt(it["count"]) }
            .bind()
    }

    suspend fun getAccessRightsForEntities(
        sub: Option<Sub>,
        entities: List<URI>
    ): Either<APIException, Map<URI, Map<AccessRight, List<SubjectRightInfo>>>> = either {
        if (entities.isEmpty())
            return@either emptyMap()

        val subjectUuids = subjectReferentialService.getSubjectAndGroupsUUID(sub).bind()

        databaseClient
            .sql(
                """
                select entity_id, sr.subject_id, access_right, subject_type, subject_info
                from entity_access_rights
                left join subject_referential sr on entity_access_rights.subject_id = sr.subject_id 
                where entity_id in (:entities_ids)
                and sr.subject_id not in (:excluded_subject_uuids);
                """.trimIndent()
            )
            .bind("entities_ids", entities)
            .bind("excluded_subject_uuids", subjectUuids)
            .allToMappedList { it }
            .groupBy { toUri(it["entity_id"]) }
            .mapValues {
                it.value
                    .groupBy { AccessRight.forAttributeName(it["access_right"] as String).getOrNull()!! }
                    .mapValues { (_, records) ->
                        records.map { record ->
                            val uuid = record["subject_id"]
                            val subjectType = toEnum<SubjectType>(record["subject_type"]!!)
                            val (uri, type) = when (subjectType) {
                                SubjectType.USER -> Pair(USER_ENTITY_PREFIX + uuid, USER_COMPACT_TYPE)
                                SubjectType.GROUP -> Pair(GROUP_ENTITY_PREFIX + uuid, GROUP_COMPACT_TYPE)
                                SubjectType.CLIENT -> Pair(CLIENT_ENTITY_PREFIX + uuid, CLIENT_COMPACT_TYPE)
                            }

                            val subjectInfo = toJsonString(record["subject_info"])
                                .deserializeAsMap()[NGSILD_VALUE_TERM] as Map<String, String>
                            val subjectSpecificInfo = when (subjectType) {
                                SubjectType.USER -> Pair(AUTH_TERM_USERNAME, subjectInfo[AUTH_TERM_USERNAME]!!)
                                SubjectType.GROUP -> Pair(AUTH_TERM_NAME, subjectInfo[AUTH_TERM_NAME]!!)
                                SubjectType.CLIENT -> Pair(AUTH_TERM_CLIENT_ID, subjectInfo[AUTH_TERM_CLIENT_ID]!!)
                            }
                            SubjectRightInfo(
                                uri = uri.toUri(),
                                subjectInfo = mapOf("kind" to type).plus(subjectSpecificInfo)
                            )
                        }
                    }
            }
    }

    suspend fun getEntitiesIdsOwnedBySubject(
        subjectId: Sub
    ): Either<APIException, List<URI>> = either {
        databaseClient
            .sql(
                """
                SELECT entity_id 
                FROM entity_access_rights
                WHERE subject_id = :sub
                AND access_right = 'isOwner'
                """.trimIndent()
            )
            .bind("sub", subjectId)
            .allToMappedList { toUri(it["entity_id"]) }
    }

    suspend fun updateSpecificAccessPolicy(
        entityId: URI,
        ngsiLdAttribute: NgsiLdAttribute
    ): Either<APIException, Unit> = either {
        val specificAccessPolicy = ngsiLdAttribute.getSpecificAccessPolicy().bind()
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET specific_access_policy = :specific_access_policy
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policy", specificAccessPolicy.toString())
            .execute()
            .bind()
    }

    suspend fun removeSpecificAccessPolicy(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET specific_access_policy = null
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()

    @Transactional
    suspend fun delete(sub: Sub): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                DELETE FROM entity_access_rights
                WHERE subject_id = :subject_id
                """.trimIndent()
            )
            .bind("subject_id", sub)
            .execute()

    @Transactional
    suspend fun deleteAllAccessRightsOnEntities(entitiesIds: List<URI>): Either<APIException, Unit> =
        databaseClient
            .sql(
                """
                DELETE FROM entity_access_rights
                WHERE entity_id IN (:entities_ids)
                """.trimIndent()
            )
            .bind("entities_ids", entitiesIds)
            .execute()

    private fun rowToEntityAccessControl(row: Map<String, Any>, isStellioAdmin: Boolean): EntityAccessRights {
        val accessRight =
            if (isStellioAdmin) CAN_ADMIN
            else (row["access_right"] as String).let { AccessRight.forAttributeName(it) }.getOrNull()!!

        return EntityAccessRights(
            id = toUri(row["entity_id"]),
            types = toList(row["types"]),
            isDeleted = row["deleted_at"] != null,
            right = accessRight,
            specificAccessPolicy = toOptionalEnum<SpecificAccessPolicy>(row["specific_access_policy"])
        )
    }
}
