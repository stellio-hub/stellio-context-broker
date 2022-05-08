package com.egm.stellio.entity.authorization

import arrow.core.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.IAM_TYPES
import com.egm.stellio.shared.util.Sub
import java.net.URI

interface AuthorizationService {
    fun getSubjectUri(sub: Option<Sub>): URI
    fun getSubjectGroups(sub: Option<Sub>): Set<URI>
    fun getAuthorizedEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        contextLink: String
    ): Pair<Int, List<JsonLdEntity>>
    fun userIsAdmin(sub: Option<Sub>): Boolean

    fun getGroupsMemberships(
        sub: Option<Sub>,
        offset: Int,
        limit: Int,
        contextLink: String
    ): Pair<Int, List<JsonLdEntity>>

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

    fun checkEntityTypesAreAuthorized(entityTypes: List<ExpandedTerm>): Either<APIException, Unit> {
        val unauthorizedTypes = IAM_TYPES.intersect(entityTypes.toSet())
        return if (unauthorizedTypes.isNotEmpty())
            BadRequestDataException("Entity type(s) $unauthorizedTypes cannot be managed via normal entity API").left()
        else Unit.right()
    }

    fun checkAttributesAreAuthorized(
        ngsiLdAttributes: List<NgsiLdAttribute>
    ): Either<APIException, Unit> =
        ngsiLdAttributes.traverse { ngsiLdAttribute ->
            checkAttributeIsAuthorized(ngsiLdAttribute.name)
        }.map {}

    fun checkAttributeIsAuthorized(attributeName: ExpandedTerm): Either<APIException, Unit> =
        if (attributeName == AuthContextModel.AUTH_PROP_SAP)
            BadRequestDataException(
                "Specific access policy cannot be updated as a normal property, " +
                    "use /ngsi-ld/v1/entityAccessControl/{entityId}/attrs/specificAccessPolicy endpoint instead"
            ).left()
        else Unit.right()

    fun isAdminAuthorized(
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userIsAdminOfEntity(entityId, sub).let {
            if (!it) AccessDeniedException("User forbidden admin access to entity $entityId").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypesAreAuthorized(entityTypes)
        }

    fun isCreationAuthorized(ngsiLdEntity: NgsiLdEntity, sub: Option<Sub>): Either<APIException, Unit> =
        userCanCreateEntities(sub).let {
            if (!it) AccessDeniedException("User forbidden to create entities").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypesAreAuthorized(ngsiLdEntity.types)
        }

    fun isUpdateAuthorized(
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userCanUpdateEntity(entityId, sub).let {
            if (!it) AccessDeniedException("User forbidden write access to entity $entityId").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypesAreAuthorized(entityTypes)
        }.flatMap {
            checkAttributesAreAuthorized(ngsiLdAttributes)
        }

    fun isUpdateAuthorized(
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        attributeName: ExpandedTerm,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userCanUpdateEntity(entityId, sub).let {
            if (!it) AccessDeniedException("User forbidden write access to entity $entityId").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypesAreAuthorized(entityTypes)
        }.flatMap {
            checkAttributeIsAuthorized(attributeName)
        }

    fun isUpdateAuthorized(ngsiLdEntity: NgsiLdEntity, sub: Option<Sub>): Either<APIException, Unit> =
        isUpdateAuthorized(ngsiLdEntity.id, ngsiLdEntity.types, ngsiLdEntity.attributes, sub)

    fun isReadAuthorized(entityId: URI, entityTypes: List<ExpandedTerm>, sub: Option<Sub>): Either<APIException, Unit> =
        userCanReadEntity(entityId, sub).let {
            if (!it) AccessDeniedException("User forbidden read access to entity $entityId").left()
            else Unit.right()
        }.flatMap {
            checkEntityTypesAreAuthorized(entityTypes)
        }
}
