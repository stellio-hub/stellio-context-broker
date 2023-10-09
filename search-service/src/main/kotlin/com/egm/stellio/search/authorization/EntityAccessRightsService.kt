package com.egm.stellio.search.authorization

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.authorization.EntityAccessRights.SubjectRightInfo
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AccessRight.*
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
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Service
class EntityAccessRightsService(
    private val applicationProperties: ApplicationProperties,
    private val databaseClient: DatabaseClient,
    private val subjectReferentialService: SubjectReferentialService,
    private val entityPayloadService: EntityPayloadService
) {
    @Transactional
    suspend fun setReadRoleOnEntity(sub: Sub, entityId: URI): Either<APIException, Unit> =
        setRoleOnEntity(sub, entityId, R_CAN_READ)

    @Transactional
    suspend fun setWriteRoleOnEntity(sub: Sub, entityId: URI): Either<APIException, Unit> =
        setRoleOnEntity(sub, entityId, R_CAN_WRITE)

    @Transactional
    suspend fun setAdminRoleOnEntity(sub: Sub, entityId: URI): Either<APIException, Unit> =
        setRoleOnEntity(sub, entityId, R_CAN_ADMIN)

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
            listOf(R_CAN_READ, R_CAN_WRITE, R_CAN_ADMIN)
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
            listOf(R_CAN_WRITE, R_CAN_ADMIN)
        ).flatMap {
            if (!it)
                AccessDeniedException("User forbidden write access to entity $entityId").left()
            else Unit.right()
        }

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
                    entityPayloadService.hasSpecificAccessPolicies(entityId, specificAccessPolicies)
                else true.right()
            }.flatMap {
                if (!it)
                    hasDirectAccessRightOnEntity(subjectUuids, entityId, accessRights)
                else true.right()
            }.bind()
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
        type: String? = null,
        limit: Int,
        offset: Int
    ): Either<APIException, List<EntityAccessRights>> = either {
        val subjectUuids = subjectReferentialService.getSubjectAndGroupsUUID(sub).bind()
        val isStellioAdmin = subjectReferentialService.hasStellioAdminRole(subjectUuids).bind()

        databaseClient
            .sql(
                """
                SELECT ep.entity_id, ep.types, ear.access_right, ep.specific_access_policy
                FROM entity_access_rights ear
                LEFT JOIN entity_payload ep ON ear.entity_id = ep.entity_id
                WHERE ${if (isStellioAdmin) "1 = 1" else "subject_id IN (:subject_uuids)" }
                ${if (accessRights.isNotEmpty()) " AND access_right in (:access_rights)" else ""}
                ${if (!type.isNullOrEmpty()) " AND ${buildTypeQuery(type)}" else ""}
                ORDER BY entity_id
                LIMIT :limit
                OFFSET :offset;
                """.trimIndent()
            )
            .bind("limit", limit)
            .bind("offset", offset)
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
            .allToMappedList { rowToEntityAccessControl(it, isStellioAdmin) }
            .groupBy { it.id }
            // a user may have multiple rights on a given entity (e.g., through groups memberships)
            // retain the one with the "higher" right
            .mapValues {
                val ear = it.value.first()
                EntityAccessRights(
                    ear.id,
                    ear.types,
                    it.value.maxOf { it.right },
                    ear.specificAccessPolicy
                )
            }.values.toList()
    }

    suspend fun getSubjectAccessRightsCount(
        sub: Option<Sub>,
        accessRights: List<AccessRight>,
        type: String? = null
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
                ${if (accessRights.isNotEmpty()) " AND access_right in (:access_rights)" else ""}
                ${if (!type.isNullOrEmpty()) " AND ${buildTypeQuery(type)}" else ""}
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
                select entity_id, sr.subject_id, access_right, subject_type, subject_info, service_account_id
                from entity_access_rights
                left join subject_referential sr 
                    on entity_access_rights.subject_id = sr.subject_id 
                    or entity_access_rights.subject_id = sr.service_account_id
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
                            val uuid = record["service_account_id"] ?: record["subject_id"]
                            val subjectType = toEnum<SubjectType>(record["subject_type"]!!)
                            val (uri, type) = when (subjectType) {
                                SubjectType.USER -> Pair(USER_ENTITY_PREFIX + uuid, USER_COMPACT_TYPE)
                                SubjectType.GROUP -> Pair(GROUP_ENTITY_PREFIX + uuid, GROUP_COMPACT_TYPE)
                                SubjectType.CLIENT -> Pair(CLIENT_ENTITY_PREFIX + uuid, CLIENT_COMPACT_TYPE)
                            }

                            val subjectInfo = toJsonString(record["subject_info"])
                                .deserializeAsMap()[JSONLD_VALUE_TERM] as Map<String, String>
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

    private fun rowToEntityAccessControl(row: Map<String, Any>, isStellioAdmin: Boolean): EntityAccessRights {
        val accessRight =
            if (isStellioAdmin) R_CAN_ADMIN
            else (row["access_right"] as String).let { AccessRight.forAttributeName(it) }.getOrNull()!!

        return EntityAccessRights(
            id = toUri(row["entity_id"]),
            types = toList(row["types"]),
            right = accessRight,
            specificAccessPolicy = toOptionalEnum<SpecificAccessPolicy>(row["specific_access_policy"])
        )
    }
}
