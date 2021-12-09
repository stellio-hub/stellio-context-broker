package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityAccessRights
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AccessRight.R_CAN_ADMIN
import com.egm.stellio.shared.util.AccessRight.R_CAN_READ
import com.egm.stellio.shared.util.AccessRight.R_CAN_WRITE
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

@Service
class EntityAccessRightsService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun setReadRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Int> =
        setRoleOnEntity(subjectId, entityId, R_CAN_READ)

    @Transactional
    fun setWriteRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Int> =
        setRoleOnEntity(subjectId, entityId, R_CAN_WRITE)

    @Transactional
    fun setRoleOnEntity(subjectId: UUID, entityId: URI, accessRight: AccessRight): Mono<Int> =
        databaseClient
            .sql(
                """
                INSERT INTO entity_access_rights (subject_id, entity_id, access_right)
                    VALUES (:subject_id, :entity_id, :access_right)
                ON CONFLICT (subject_id, entity_id, access_right)
                    DO UPDATE SET access_right = :access_right
                """
            )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .bind("access_right", accessRight.toString())
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorResume {
                logger.error("Error while setting access right on entity: $it")
                Mono.just(-1)
            }

    @Transactional
    fun removeRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Int> =
        databaseClient
            .sql(
                """
                DELETE from entity_access_rights
                WHERE subject_id = :subject_id
                AND entity_id = :entity_id
                """
            )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorReturn(-1)

    fun hasReadRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Boolean> =
        hasRoleOnEntity(subjectId, entityId, listOf(R_CAN_READ, R_CAN_WRITE, R_CAN_ADMIN))

    fun hasWriteRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Boolean> =
        hasRoleOnEntity(subjectId, entityId, listOf(R_CAN_WRITE, R_CAN_ADMIN))

    fun hasRoleOnEntity(subjectId: UUID, entityId: URI, accessRights: List<AccessRight>): Mono<Boolean> =
        databaseClient
            .sql(
                """
                SELECT COUNT(subject_id) as count
                FROM entity_access_rights
                WHERE subject_id = :subject_id
                AND entity_id = :entity_id
                AND access_right IN(:access_rights)
                """
            )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .bind("access_rights", accessRights.map { it.toString() })
            .fetch()
            .one()
            .map {
                it["count"] as Long == 1L
            }
            .onErrorResume {
                logger.error("Error while checking role on entity: $it")
                Mono.just(false)
            }

    @Transactional
    fun delete(subjectId: UUID): Mono<Int> =
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .matching(Query.query(Criteria.where("subject_id").`is`(subjectId)))
            .all()
}
