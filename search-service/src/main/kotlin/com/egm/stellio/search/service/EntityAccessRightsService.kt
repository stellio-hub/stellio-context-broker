package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityAccessRights
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AccessRight.R_CAN_ADMIN
import com.egm.stellio.shared.util.AccessRight.R_CAN_READ
import com.egm.stellio.shared.util.AccessRight.R_CAN_WRITE
import kotlinx.coroutines.reactive.awaitFirst
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
    private val subjectReferentialService: SubjectReferentialService
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
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .bind("access_right", accessRight.attributeName)
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
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorReturn(-1)

    fun canReadEntity(subjectId: UUID, entityId: URI): Mono<Boolean> =
        checkHasAccessRight(subjectId, entityId, listOf(R_CAN_READ, R_CAN_WRITE, R_CAN_ADMIN))

    fun canWriteEntity(subjectId: UUID, entityId: URI): Mono<Boolean> =
        checkHasAccessRight(subjectId, entityId, listOf(R_CAN_WRITE, R_CAN_ADMIN))

    private fun checkHasAccessRight(subjectId: UUID, entityId: URI, accessRights: List<AccessRight>): Mono<Boolean> =
        subjectReferentialService.hasStellioAdminRole(subjectId)
            .flatMap {
                // if user has stellio-admin role, no need to check further
                if (it) Mono.just(true)
                else {
                    subjectReferentialService.getSubjectAndGroupsUUID(subjectId)
                        .flatMap { uuids ->
                            // ... and check if it has the required role with at least one of them
                            hasAccessRightOnEntity(uuids, entityId, accessRights)
                        }
                }
            }

    private fun hasAccessRightOnEntity(
        uuids: List<UUID>,
        entityId: URI,
        accessRights: List<AccessRight>
    ): Mono<Boolean> =
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
            .fetch()
            .one()
            .map {
                it["count"] as Long >= 1L
            }
            .onErrorResume {
                logger.error("Error while checking role on entity: $it")
                Mono.just(false)
            }

    suspend fun computeAccessRightFilter(subjectId: UUID): () -> String? {
        if (subjectReferentialService.hasStellioAdminRole(subjectId).awaitFirst())
            return { null }
        else {
            val subjectAndGroupsUUID = subjectReferentialService.getSubjectAndGroupsUUID(subjectId).awaitFirst()
            val formattedSubjectAndGroupsUUID = subjectAndGroupsUUID.joinToString(",") { "'$it'" }
            return {
                """
                    entity_id IN (
                        SELECT entity_id
                        FROM entity_access_rights
                        WHERE subject_id IN ($formattedSubjectAndGroupsUUID)
                    )
                """.trimIndent()
            }
        }
    }

    @Transactional
    fun delete(subjectId: UUID): Mono<Int> =
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .matching(Query.query(Criteria.where("subject_id").`is`(subjectId)))
            .all()
}
