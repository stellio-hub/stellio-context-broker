package com.egm.stellio.search.service

import arrow.core.getOrElse
import com.egm.stellio.search.model.SubjectReferential
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.toUUID
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
                    (subject_id, subject_type, service_account_id, global_roles, groups_memberships)
                    VALUES (:subject_id, :subject_type, :service_account_id, :global_roles, :groups_memberships)
                ON CONFLICT (subject_id)
                    DO UPDATE SET service_account_id = :service_account_id,
                        global_roles = :global_roles,
                        groups_memberships = :groups_memberships
                """.trimIndent()
            )
            .bind("subject_id", subjectReferential.subjectId)
            .bind("subject_type", subjectReferential.subjectType.toString())
            .bind("service_account_id", subjectReferential.serviceAccountId)
            .bind("global_roles", subjectReferential.globalRoles?.map { it.key }?.toTypedArray())
            .bind("groups_memberships", subjectReferential.groupsMemberships?.toTypedArray())
            .fetch()
            .rowsUpdated()
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
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .fetch()
            .one()
            .map { rowToSubjectReferential(it) }

    fun getSubjectAndGroupsUUID(subjectId: UUID): Mono<List<UUID>> =
        databaseClient
            .sql(
                """
                SELECT subject_id, groups_memberships
                FROM subject_referential
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .fetch()
            .one()
            .map {
                ((it["groups_memberships"] as Array<String>?)?.map { it.toUUID() } ?: emptyList())
                    .plus(it["subject_id"] as UUID)
            }

    fun hasStellioAdminRole(subjectId: UUID): Mono<Boolean> =
        databaseClient
            .sql(
                """
                SELECT COUNT(subject_id) as count
                FROM subject_referential
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                AND '${GlobalRole.STELLIO_ADMIN.key}' = ANY(global_roles)
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .fetch()
            .one()
            .map {
                it["count"] as Long == 1L
            }
            .onErrorResume {
                logger.error("Error while checking stellio-admin role for user: $it")
                Mono.just(false)
            }

    @Transactional
    fun setGlobalRoles(subjectId: UUID, newRoles: List<GlobalRole>): Mono<Int> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET global_roles = :global_roles
                WHERE subject_id = :subject_id
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .bind("global_roles", newRoles.map { it.key }.toTypedArray())
            .fetch()
            .rowsUpdated()
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
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .fetch()
            .rowsUpdated()
            .onErrorResume {
                logger.error("Error while resetting global roles: $it")
                Mono.just(-1)
            }

    @Transactional
    fun addGroupMembershipToUser(subjectId: UUID, groupId: UUID): Mono<Int> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET groups_memberships = array_append(groups_memberships, :group_id::text)
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .bind("group_id", groupId)
            .fetch()
            .rowsUpdated()
            .onErrorResume {
                logger.error("Error while adding group membership to user: $it")
                Mono.just(-1)
            }

    @Transactional
    fun removeGroupMembershipToUser(subjectId: UUID, groupId: UUID): Mono<Int> =
        databaseClient
            .sql(
                """
                UPDATE subject_referential
                SET groups_memberships = array_remove(groups_memberships, :group_id::text)
                WHERE (subject_id = :subject_id OR service_account_id = :subject_id)
                """.trimIndent()
            )
            .bind("subject_id", subjectId)
            .bind("group_id", groupId)
            .fetch()
            .rowsUpdated()
            .onErrorResume {
                logger.error("Error while removing group membership to user: $it")
                Mono.just(-1)
            }

    @Transactional
    fun addServiceAccountIdToClient(subjectId: UUID, serviceAccountId: UUID): Mono<Int> =
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
            .fetch()
            .rowsUpdated()
            .onErrorResume {
                logger.error("Error while setting service account id to client: $it")
                Mono.just(-1)
            }

    @Transactional
    fun delete(subjectId: UUID): Mono<Int> =
        r2dbcEntityTemplate.delete(SubjectReferential::class.java)
            .matching(Query.query(Criteria.where("subject_id").`is`(subjectId)))
            .all()

    private fun rowToSubjectReferential(row: Map<String, Any>) =
        SubjectReferential(
            subjectId = row["subject_id"] as UUID,
            subjectType = SubjectType.valueOf(row["subject_type"] as String),
            serviceAccountId = row["service_account_id"] as UUID?,
            globalRoles = (row["global_roles"] as Array<String>?)
                ?.mapNotNull { GlobalRole.forKey(it).getOrElse { null } },
            groupsMemberships = (row["groups_memberships"] as Array<String>?)?.map { it.toUUID() }
        )
}
