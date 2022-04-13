package com.egm.stellio.entity.authorization

import arrow.core.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.IAM_TYPES
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.toUri
import org.neo4j.driver.internal.value.DateTimeValue
import org.neo4j.driver.internal.value.StringValue
import org.neo4j.driver.types.Node
import java.net.URI

interface AuthorizationService {
    fun getSubjectUri(sub: Option<Sub>): URI
    fun getSubjectGroups(sub: Option<Sub>): Set<URI>
    fun getAuthorizedEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<EntityAccessControl>>
    fun userIsAdmin(sub: Option<Sub>): Boolean
    fun userCanCreateEntities(sub: Option<Sub>): Boolean
    fun filterEntitiesUserCanRead(entitiesId: List<URI>, sub: Option<Sub>): List<URI>
    fun filterEntitiesUserCanUpdate(entitiesId: List<URI>, sub: Option<Sub>): List<URI>
    fun filterEntitiesUserCanAdmin(entitiesId: List<URI>, sub: Option<Sub>): List<URI>
    fun splitEntitiesByUserCanAdmin(entitiesId: List<URI>, sub: Option<Sub>): Pair<List<URI>, List<URI>>
    fun userCanReadEntity(entityId: URI, sub: Option<Sub>): Boolean
    fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Boolean
    fun userIsAdminOfEntity(entityId: URI, sub: Option<Sub>): Boolean
    fun createAdminLink(entityId: URI, sub: Option<Sub>)
    fun createAdminLinks(entitiesId: List<URI>, sub: Option<Sub>)
    fun removeUserRightsOnEntity(entityId: URI, subjectId: URI): Int

    fun checkEntityTypeIsAuthorized(entityType: ExpandedTerm): Either<APIException, Unit> =
        if (IAM_TYPES.contains(entityType))
            BadRequestDataException("Entity type $entityType cannot be managed via normal entity API").left()
        else Unit.right()

    fun checkAttributesAreAuthorized(
        ngsiLdAttributes: List<NgsiLdAttribute>
    ): Either<APIException, Unit> =
        ngsiLdAttributes.traverseEither { ngsiLdAttribute ->
            checkAttributeIsAuthorized(ngsiLdAttribute.name)
        }.map { it.first() }

    fun checkAttributeIsAuthorized(attributeName: ExpandedTerm): Either<APIException, Unit> =
        if (attributeName == AuthContextModel.AUTH_PROP_SAP)
            BadRequestDataException(
                "Specific access policy cannot be updated as a normal property, " +
                    "use /ngsi-ld/v1/entityAccessControl/{entityId}/attrs/specificAccessPolicy endpoint instead"
            ).left()
        else Unit.right()

    fun isAdminAuthorized(entityId: URI, entityType: ExpandedTerm, sub: Option<Sub>): Either<APIException, Unit> =
        userIsAdminOfEntity(entityId, sub).let {
            if (!it) AccessDeniedException("User forbidden admin access to entity $entityId").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypeIsAuthorized(entityType)
        }

    fun isCreationAuthorized(ngsiLdEntity: NgsiLdEntity, sub: Option<Sub>): Either<APIException, Unit> =
        userCanCreateEntities(sub).let {
            if (!it) AccessDeniedException("User forbidden to create entities").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypeIsAuthorized(ngsiLdEntity.type)
        }

    fun isUpdateAuthorized(
        entityId: URI,
        entityType: ExpandedTerm,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userCanUpdateEntity(entityId, sub).let {
            if (!it) AccessDeniedException("User forbidden write access to entity $entityId").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypeIsAuthorized(entityType)
        }.flatMap {
            checkAttributesAreAuthorized(ngsiLdAttributes)
        }

    fun isUpdateAuthorized(
        entityId: URI,
        entityType: ExpandedTerm,
        attributeName: ExpandedTerm,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userCanUpdateEntity(entityId, sub).let {
            if (!it) AccessDeniedException("User forbidden write access to entity $entityId").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypeIsAuthorized(entityType)
        }.flatMap {
            checkAttributeIsAuthorized(attributeName)
        }

    fun isUpdateAuthorized(ngsiLdEntity: NgsiLdEntity, sub: Option<Sub>): Either<APIException, Unit> =
        isUpdateAuthorized(ngsiLdEntity.id, ngsiLdEntity.type, ngsiLdEntity.attributes, sub)

    fun isReadAuthorized(entityId: URI, entityType: ExpandedTerm, sub: Option<Sub>): Either<APIException, Unit> =
        userCanReadEntity(entityId, sub).let {
            if (!it) AccessDeniedException("User forbidden read access to entity $entityId").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypeIsAuthorized(entityType)
        }

    fun prepareResultsAuthorizedEntities(
        limit: Int,
        result: Collection<Map<String, Any>>
    ): Pair<Int, List<EntityAccessControl>> =
        if (limit == 0)
            Pair(
                (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
                emptyList()
            )
        else Pair(
            (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
            constructEntityAccessControl((result.firstOrNull()?.get("entities") as List<Map<String, Any>>))
        )

    fun constructEntityAccessControl(entities: List<Map<String, Any>>): List<EntityAccessControl> =
        entities.map {
            val entityNode = it["entity"] as Node
            val rightOnEntity = (it["right"] as org.neo4j.driver.types.Relationship).type()
            val specificAccessPolicy = it["specificAccessPolicy"] as String?
            val entityId = (entityNode.get("id") as StringValue).asString().toUri()

            if(specificAccessPolicy!=null){
                EntityAccessControl(
                    id = entityId,
                    type = entityNode.labels() as List<String>,
                    right = AccessRight.forAttributeName(rightOnEntity).orNull()!!,
                    createdAt = (entityNode.get("createdAt") as DateTimeValue).asZonedDateTime(),
                    modifiedAt = (entityNode?.get("modifiedAt") as DateTimeValue)?.asZonedDateTime(),
                    specificAccessPolicy = AuthContextModel.SpecificAccessPolicy.valueOf(specificAccessPolicy)
                )
            }
            else {
                EntityAccessControl(
                    id = entityId,
                    type = entityNode.labels() as List<String>,
                    right = AccessRight.forAttributeName(rightOnEntity).orNull()!!,
                    createdAt = (entityNode.get("createdAt") as DateTimeValue).asZonedDateTime(),
                    modifiedAt = (entityNode?.get("modifiedAt") as DateTimeValue)?.asZonedDateTime(),
                )
            }
        }

    fun searchAuthorizedEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        offset: Int,
        limit: Int,
        contextLink: String,
        includeSysAttrs: Boolean
    ): Pair<Int, List<JsonLdEntity>> {
        val result = getAuthorizedEntities(
            queryParams,
            sub,
            offset,
            limit,
            listOf(contextLink)
        )

        val jsonLdEntities = result.second.map {
            JsonLdEntity(
                it.serializeCoreProperties(includeSysAttrs),
                it.contexts
            )
        }

        return Pair(result.first, jsonLdEntities)
    }
}
