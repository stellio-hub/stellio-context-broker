package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.CREATION_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_PREFIX
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.WRITE_RIGHT
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled")
class Neo4jAuthorizationService(
    private val neo4jAuthorizationRepository: Neo4jAuthorizationRepository,
    private val neo4jRepository: Neo4jRepository
) : AuthorizationService {

    override fun userIsAdmin(userId: String): Boolean = userIsOneOfGivenRoles(ADMIN_ROLES, userId)

    override fun userCanCreateEntities(userId: String): Boolean = userIsOneOfGivenRoles(CREATION_ROLES, userId)

    private fun userIsOneOfGivenRoles(roles: Set<String>, userId: String): Boolean =
        neo4jAuthorizationRepository.getUserRoles(USER_PREFIX + userId).intersect(roles).isNotEmpty()

    override fun filterEntitiesUserCanRead(entitiesId: List<String>, userId: String): List<String> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, READ_RIGHT, userId)

    override fun filterEntitiesUserCanWrite(entitiesId: List<String>, userId: String): List<String> =
        filterEntitiesUserHaveOneOfGivenRights(entitiesId, WRITE_RIGHT, userId)

    private fun filterEntitiesUserHaveOneOfGivenRights(
        entitiesId: List<String>,
        rights: Set<String>,
        userId: String
    ): List<String> =
        if (userIsAdmin(userId))
            entitiesId
        else neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            USER_PREFIX + userId,
            entitiesId,
            rights
        )

    override fun userCanReadEntity(entityId: String, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, READ_RIGHT, userId)

    override fun userCanWriteEntity(entityId: String, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, WRITE_RIGHT, userId)

    override fun userCanAdminEntity(entityId: String, userId: String): Boolean =
        userHasOneOfGivenRightsOnEntity(entityId, ADMIN_RIGHT, userId)

    private fun userHasOneOfGivenRightsOnEntity(
        entityId: String,
        rights: Set<String>,
        userId: String
    ): Boolean =
        userIsAdmin(userId) || neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
            USER_PREFIX + userId,
            listOf(entityId),
            rights
        ).isNotEmpty()

    override fun createAdminLink(entityId: String, userId: String) {
        neo4jRepository.createRelationshipOfSubject(
            EntitySubjectNode(USER_PREFIX + userId),
            Relationship(
                type = listOf(R_CAN_ADMIN),
                datasetId = URI.create("urn:ngsi-ld:Dataset:rCanAdmin:$entityId")
            ),
            entityId
        )
    }
}
