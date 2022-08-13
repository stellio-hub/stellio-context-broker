package com.egm.stellio.search.authorization

import arrow.core.*
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.EntityAccessRights
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.util.execute
import com.egm.stellio.search.util.oneToResult
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AccessRight.*
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.Sub
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Service
class EntityAccessRightsService(
    private val applicationProperties: ApplicationProperties,
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val subjectReferentialService: SubjectReferentialService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService
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
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .matching(
                Query.query(
                    Criteria.where("subject_id").`is`(sub)
                        .and(Criteria.where("entity_id").`is`(entityId))
                )
            )
            .execute()

    @Transactional
    suspend fun removeRolesOnEntity(entityId: URI): Either<APIException, Unit> =
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .matching(
                Query.query(
                    Criteria.where("entity_id").`is`(entityId)
                )
            )
            .execute()

    suspend fun canReadEntity(sub: Option<Sub>, entityId: URI): Either<APIException, Unit> =
        checkHasRightOnEntity(
            sub,
            entityId,
            listOf(AuthContextModel.SpecificAccessPolicy.AUTH_READ, AuthContextModel.SpecificAccessPolicy.AUTH_WRITE),
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
            listOf(AuthContextModel.SpecificAccessPolicy.AUTH_WRITE),
            listOf(R_CAN_WRITE, R_CAN_ADMIN)
        ).flatMap {
            if (!it)
                AccessDeniedException("User forbidden write access to entity $entityId").left()
            else Unit.right()
        }

    internal suspend fun checkHasRightOnEntity(
        sub: Option<Sub>,
        entityId: URI,
        specificAccessPolicies: List<AuthContextModel.SpecificAccessPolicy>,
        accessRights: List<AccessRight>
    ): Either<APIException, Boolean> {
        if (!applicationProperties.authentication.enabled)
            return true.right()

        return subjectReferentialService.hasStellioAdminRole(sub)
            .flatMap {
                if (!it)
                    temporalEntityAttributeService.hasSpecificAccessPolicies(entityId, specificAccessPolicies)
                else true.right()
            }
            .flatMap {
                if (!it)
                    subjectReferentialService.getSubjectAndGroupsUUID(sub)
                        .flatMap { uuids -> hasDirectAccessRightOnEntity(uuids, entityId, accessRights) }
                else true.right()
            }
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

    @Transactional
    suspend fun delete(sub: Sub): Either<APIException, Unit> =
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .matching(Query.query(Criteria.where("subject_id").`is`(sub)))
            .execute()
}
