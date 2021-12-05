package com.egm.stellio.search.service

import com.egm.stellio.search.model.SubjectAccessRights
import com.egm.stellio.shared.util.ADMIN_ROLE_LABEL
import com.egm.stellio.shared.util.SubjectType
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

@Service
class SubjectAccessRightsService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun create(subjectAccessRights: SubjectAccessRights): Mono<Int> =
        databaseClient.sql(
            """
            INSERT INTO subject_access_rights
                (subject_id, subject_type, global_role, allowed_read_entities, allowed_write_entities)
            VALUES (:subject_id, :subject_type, :global_role, :allowed_read_entities, :allowed_write_entities)
            """
        )
            .bind("subject_id", subjectAccessRights.subjectId)
            .bind("subject_type", subjectAccessRights.subjectType.toString())
            .bind("global_role", subjectAccessRights.globalRole)
            .bind("allowed_read_entities", subjectAccessRights.allowedReadEntities)
            .bind("allowed_write_entities", subjectAccessRights.allowedWriteEntities)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorResume {
                logger.error("Error while creating a new subject access right : ${it.message}", it)
                Mono.just(-1)
            }

    fun retrieve(subjectId: UUID): Mono<SubjectAccessRights> =
        databaseClient.sql(
            """
            SELECT *
            FROM subject_access_rights
            WHERE subject_id = :subject_id                
            """
        )
            .bind("subject_id", subjectId)
            .fetch()
            .one()
            .map { rowToUserAccessRights(it) }

    @Transactional
    fun addReadRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Int> =
        databaseClient.sql(
            """
                UPDATE subject_access_rights
                SET allowed_read_entities = array_append(allowed_read_entities, :entity_id::text)
                WHERE subject_id = :subject_id
            """
        )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorReturn(-1)

    @Transactional
    fun addWriteRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Int> =
        databaseClient.sql(
            """
                UPDATE subject_access_rights
                SET allowed_write_entities = array_append(allowed_write_entities, :entity_id::text)
                WHERE subject_id = :subject_id
            """
        )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorReturn(-1)

    @Transactional
    fun removeRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Int> =
        databaseClient.sql(
            """
                UPDATE subject_access_rights
                SET allowed_read_entities = array_remove(allowed_read_entities, :entity_id::text),
                    allowed_write_entities = array_remove(allowed_write_entities, :entity_id::text)
                WHERE subject_id = :subject_id
            """
        )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorReturn(-1)

    @Transactional
    fun addAdminGlobalRole(subjectId: UUID): Mono<Int> =
        databaseClient.sql(
            """
                UPDATE subject_access_rights
                SET global_role = '$ADMIN_ROLE_LABEL'
                WHERE subject_id = :subject_id
            """
        )
            .bind("subject_id", subjectId)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorReturn(-1)

    @Transactional
    fun removeAdminGlobalRole(subjectId: UUID): Mono<Int> =
        databaseClient.sql(
            """
                UPDATE subject_access_rights
                SET global_role = null
                WHERE subject_id = :subject_id
            """
        )
            .bind("subject_id", subjectId)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorReturn(-1)

    fun hasReadRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Boolean> =
        databaseClient.sql(
            """
                SELECT COUNT(subject_id) as count
                FROM subject_access_rights
                WHERE subject_id = :subject_id
                AND (:entity_id = ANY(allowed_read_entities) OR :entity_id = ANY(allowed_write_entities))
            """
        )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .fetch()
            .one()
            .map {
                it["count"] as Long == 1L
            }
            .onErrorReturn(false)

    fun hasWriteRoleOnEntity(subjectId: UUID, entityId: URI): Mono<Boolean> =
        databaseClient.sql(
            """
                SELECT COUNT(subject_id) as count
                FROM subject_access_rights
                WHERE subject_id = :subject_id
                AND (:entity_id = ANY(allowed_write_entities))
            """
        )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .fetch()
            .one()
            .map {
                it["count"] as Long == 1L
            }
            .onErrorReturn(false)

    @Transactional
    fun delete(subjectId: UUID): Mono<Int> =
        r2dbcEntityTemplate.delete(SubjectAccessRights::class.java)
            .matching(Query.query(Criteria.where("subject_id").`is`(subjectId)))
            .all()

    private fun rowToUserAccessRights(row: Map<String, Any>) =
        SubjectAccessRights(
            subjectId = row["subject_id"] as UUID,
            subjectType = SubjectType.valueOf(row["subject_type"] as String),
            globalRole = row["global_role"] as String?,
            allowedReadEntities = (row["allowed_read_entities"] as Array<String>?),
            allowedWriteEntities = (row["allowed_write_entities"] as Array<String>?)
        )
}
