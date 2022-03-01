package com.egm.stellio.entity.authorization

import arrow.core.Either
import arrow.core.Option
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.IAM_TYPES
import com.egm.stellio.shared.util.Sub
import java.net.URI

interface AuthorizationService {
    fun getSubjectUri(sub: Option<Sub>): URI
    fun getSubjectGroups(sub: Option<Sub>): Set<URI>
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

    fun checkEntityTypeIsAuthorized(entityType: ExpandedTerm): Either<APiException, Unit> =
        if (IAM_TYPES.contains(entityType))
            BadRequestDataException("Entity type $entityType cannot be managed via normal entity API").left()
        else Unit.right()

    fun checkAttributesAreAuthorized(
        ngsiLdAttributes: List<NgsiLdAttribute>,
        entityUri: URI
    ) = ngsiLdAttributes.forEach { ngsiLdAttribute ->
        checkAttributeIsAuthorized(ngsiLdAttribute.name, entityUri)
    }

    fun checkAttributeIsAuthorized(attributeName: ExpandedTerm, entityUri: URI) {
        if (attributeName == AuthContextModel.AUTH_PROP_SAP)
            throw BadRequestDataException(
                "Specific access policy cannot be updated as a normal property, " +
                    "use /ngsi-ld/v1/entityAccessControl/{entityId}/attrs/specificAccessPolicy endpoint instead"
            )
    }

    fun isCreationAuthorized(ngsiLdEntity: NgsiLdEntity, sub: Option<Sub>): Either<APiException, Unit> =
        userCanCreateEntities(sub).let {
            if (it) AccessDeniedException("User forbidden to create entities").left()
            else Unit.right()
        }.map {
            checkEntityTypeIsAuthorized(ngsiLdEntity.type)
        }
}
