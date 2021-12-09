package com.egm.stellio.search.service

import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.shared.util.GlobalRole
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
import java.util.UUID

@Service
class SubjectReferentialService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun create(subjectReferential: SubjectReferential): Mono<Int> =
        databaseClient
            .sql(
                """
                INSERT INTO subject_referential
                    (subject_id, subject_type, global_roles, groups_memberships)
                VALUES (:subject_id, :subject_type, :global_roles, :groups_memberships)
                """
            )
            .bind("subject_id", subjectReferential.subjectId)
            .bind("subject_type", subjectReferential.subjectType.toString())
            .bind("global_roles", subjectReferential.globalRoles?.map { it.toString() }?.toTypedArray())
            .bind("groups_memberships", subjectReferential.groupsMemberships?.toTypedArray())
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorResume {
                logger.error("Error while creating a new subject referential : ${it.message}", it)
                Mono.just(-1)
            }

    fun retrieve(subjectId: UUID): Mono<SubjectReferential> =
        databaseClient
            .sql(
                """
                SELECT *
                FROM subject_referential
                WHERE subject_id = :subject_id                
                """
            )
            .bind("subject_id", subjectId)
            .fetch()
            .one()
            .map { rowToUserAccessRights(it) }

    @Transactional
    fun setGlobalRoles(subjectId: UUID, newRoles: List<GlobalRole>): Mono<Int> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET global_roles = :global_roles
                WHERE subject_id = :subject_id
                """
            )
            .bind("subject_id", subjectId)
            .bind("global_roles", newRoles.map { it.toString() }.toTypedArray())
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorResume {
                logger.error("Error while setting global roles: $it")
                Mono.just(-1)
            }

    @Transactional
    fun resetGlobalRoles(subjectId: UUID): Mono<Int> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET global_roles = null
                WHERE subject_id = :subject_id
                """
            )
            .bind("subject_id", subjectId)
            .fetch()
            .rowsUpdated()
            .thenReturn(1)
            .onErrorResume {
                logger.error("Error while resetting global roles: $it")
                Mono.just(-1)
            }

    @Transactional
    fun delete(subjectId: UUID): Mono<Int> =
        r2dbcEntityTemplate.delete(SubjectReferential::class.java)
            .matching(Query.query(Criteria.where("subject_id").`is`(subjectId)))
            .all()

    private fun rowToUserAccessRights(row: Map<String, Any>) =
        SubjectReferential(
            subjectId = row["subject_id"] as UUID,
            subjectType = SubjectType.valueOf(row["subject_type"] as String),
            globalRoles = (row["global_roles"] as Array<String>?)?.map { GlobalRole.valueOf(it) },
            groupsMemberships = (row["groups_memberships"] as Array<UUID>?)?.toList()
        )
}
