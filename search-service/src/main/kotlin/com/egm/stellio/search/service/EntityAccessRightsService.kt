package com.egm.stellio.search.service

import arrow.core.Option
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.EntityAccessRights
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AccessRight.R_CAN_ADMIN
import com.egm.stellio.shared.util.AccessRight.R_CAN_READ
import com.egm.stellio.shared.util.AccessRight.R_CAN_WRITE
import com.egm.stellio.shared.util.Sub
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.net.URI

@Service
class EntityAccessRightsService(
    private val applicationProperties: ApplicationProperties,
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val subjectReferentialService: SubjectReferentialService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun setReadRoleOnEntity(sub: Sub, entityId: URI): Mono<Int> =
        setRoleOnEntity(sub, entityId, R_CAN_READ)

    @Transactional
    fun setWriteRoleOnEntity(sub: Sub, entityId: URI): Mono<Int> =
        setRoleOnEntity(sub, entityId, R_CAN_WRITE)

    @Transactional
    fun setRoleOnEntity(sub: Sub, entityId: URI, accessRight: AccessRight): Mono<Int> =
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
            .fetch()
            .rowsUpdated()
            .onErrorResume {
                logger.error("Error while setting access right on entity: $it")
                Mono.just(-1)
            }

    @Transactional
    fun removeRoleOnEntity(sub: Sub, entityId: URI): Mono<Int> =
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .matching(
                Query.query(
                    Criteria.where("subject_id").`is`(sub)
                        .and(Criteria.where("entity_id").`is`(entityId))
                )
            )
            .all()
            .onErrorResume {
                logger.error("Error while removing access right on entity: $it")
                Mono.just(-1)
            }

    fun canReadEntity(sub: Option<Sub>, entityId: URI): Mono<Boolean> =
        checkHasAccessRight(sub, entityId, listOf(R_CAN_READ, R_CAN_WRITE, R_CAN_ADMIN))

    fun canWriteEntity(sub: Option<Sub>, entityId: URI): Mono<Boolean> =
        checkHasAccessRight(sub, entityId, listOf(R_CAN_WRITE, R_CAN_ADMIN))

    private fun checkHasAccessRight(sub: Option<Sub>, entityId: URI, accessRights: List<AccessRight>): Mono<Boolean> =
        Mono.just(!applicationProperties.authentication.enabled)
            .flatMap {
                if (it) Mono.just(true)
                else subjectReferentialService.hasStellioAdminRole(sub)
            }
            .flatMap {
                // if user has stellio-admin role, no need to check further
                if (it) Mono.just(true)
                else {
                    subjectReferentialService.getSubjectAndGroupsUUID(sub)
                        .flatMap { uuids ->
                            // ... and check if it has the required role with at least one of them
                            hasAccessRightOnEntity(uuids, entityId, accessRights)
                        }
                        .switchIfEmpty { Mono.just(false) }
                }
            }

    private fun hasAccessRightOnEntity(
        uuids: List<Sub>,
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

    suspend fun computeAccessRightFilter(sub: Option<Sub>): () -> String? {
        if (!applicationProperties.authentication.enabled ||
            subjectReferentialService.hasStellioAdminRole(sub).awaitFirst()
        )
            return { null }
        else {
            return subjectReferentialService.getSubjectAndGroupsUUID(sub)
                .map {
                    {
                        """
                        entity_id IN (
                            SELECT entity_id
                            FROM entity_access_rights
                            WHERE subject_id IN (${it.toListOfString()})
                        )
                        """.trimIndent()
                    }
                }.switchIfEmpty {
                    Mono.just {
                        "entity_id IN ('None')"
                    }
                }.awaitFirst()
        }
    }

    private fun <T> List<T>.toListOfString() = this.joinToString(",") { "'$it'" }

    @Transactional
    fun delete(sub: Sub): Mono<Int> =
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .matching(Query.query(Criteria.where("subject_id").`is`(sub)))
            .all()
}
