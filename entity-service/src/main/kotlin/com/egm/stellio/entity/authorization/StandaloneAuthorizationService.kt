package com.egm.stellio.entity.authorization

import arrow.core.Option
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.toUri
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class StandaloneAuthorizationService(
    private val neo4jAuthorizationRepository: Neo4jAuthorizationRepository
) : AuthorizationService {
    override fun getSubjectUri(sub: Option<Sub>): URI = (USER_TYPE + "None").toUri()

    override fun getSubjectGroups(sub: Option<Sub>): Set<URI> = emptySet()

    override fun getAuthorizedEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<EntityAccessControl>>{
        val result = neo4jAuthorizationRepository.getAuthorizedEntitiesWithoutAuthentication(queryParams, offset, limit)
        return prepareResultsAuthorizedEntities(limit, result)
    }

    override fun userIsAdmin(sub: Option<Sub>): Boolean {
        return true
    }

    override fun userCanCreateEntities(sub: Option<Sub>): Boolean {
        return true
    }

    override fun filterEntitiesUserCanRead(entitiesId: List<URI>, sub: Option<Sub>): List<URI> {
        return entitiesId
    }

    override fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, sub: Option<Sub>): List<URI> {
        return entitiesId
    }

    override fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, sub: Option<Sub>): List<URI> {
        return entitiesId
    }

    override fun splitEntitiesByUserCanAdmin(
        entitiesId: List<URI>,
        sub: Option<Sub>
    ): Pair<List<URI>, List<URI>> {
        return Pair(entitiesId, emptyList())
    }

    override fun userCanReadEntity(entityId: URI, sub: Option<Sub>): Boolean {
        return true
    }

    override fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Boolean {
        return true
    }

    override fun userIsAdminOfEntity(entityId: URI, sub: Option<Sub>): Boolean {
        return true
    }

    override fun createAdminLink(entityId: URI, sub: Option<Sub>) {}

    override fun createAdminLinks(entitiesId: List<URI>, sub: Option<Sub>) {}

    override fun removeUserRightsOnEntity(entityId: URI, subjectId: URI): Int = 1
}
