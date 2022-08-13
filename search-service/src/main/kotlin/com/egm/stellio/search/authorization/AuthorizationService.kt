package com.egm.stellio.search.authorization

import arrow.core.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.IAM_TYPES
import com.egm.stellio.shared.util.Sub
import java.net.URI

interface AuthorizationService {
    suspend fun computeAccessRightFilter(sub: Option<Sub>): () -> String?

    // FIXME should not be exposed
    suspend fun userIsAdmin(sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanCreateEntities(sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanReadEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userCanUpdateEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun userIsAdminOfEntity(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>

    suspend fun createAdminLink(entityId: URI, sub: Option<Sub>): Either<APIException, Unit>
    suspend fun createAdminLinks(entitiesId: List<URI>, sub: Option<Sub>): Either<APIException, Unit>

    private fun checkEntityTypesAreAuthorized(entityTypes: List<ExpandedTerm>): Either<APIException, Unit> {
        val unauthorizedTypes = IAM_TYPES.intersect(entityTypes.toSet())
        return if (unauthorizedTypes.isNotEmpty())
            BadRequestDataException("Entity type(s) $unauthorizedTypes cannot be managed via normal entity API").left()
        else Unit.right()
    }

    private fun checkAttributesAreAuthorized(
        ngsiLdAttributes: List<NgsiLdAttribute>
    ): Either<APIException, Unit> =
        ngsiLdAttributes.traverse { ngsiLdAttribute ->
            checkAttributeIsAuthorized(ngsiLdAttribute.name)
        }.map {}

    private fun checkAttributesAreAuthorized(
        jsonLdAttributes: Map<String, Any>
    ): Either<APIException, Unit> =
        jsonLdAttributes.keys.traverse { expandedAttributeName ->
            checkAttributeIsAuthorized(expandedAttributeName)
        }.map {}

    private fun checkAttributeIsAuthorized(attributeName: ExpandedTerm): Either<APIException, Unit> =
        if (attributeName == AuthContextModel.AUTH_PROP_SAP)
            BadRequestDataException(
                "Specific access policy cannot be updated as a normal property, " +
                    "use /ngsi-ld/v1/entityAccessControl/{entityId}/attrs/specificAccessPolicy endpoint instead"
            ).left()
        else Unit.right()

    suspend fun checkAdminAuthorized(
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userIsAdminOfEntity(entityId, sub).flatMap {
            checkEntityTypesAreAuthorized(entityTypes)
        }

    suspend fun checkCreationAuthorized(ngsiLdEntity: NgsiLdEntity, sub: Option<Sub>): Either<APIException, Unit> =
        userCanCreateEntities(sub).flatMap {
            checkEntityTypesAreAuthorized(ngsiLdEntity.types)
        }

    suspend fun checkUpdateAuthorized(
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userCanUpdateEntity(entityId, sub)
            .flatMap {
                checkEntityTypesAreAuthorized(entityTypes)
            }.flatMap {
                checkAttributesAreAuthorized(ngsiLdAttributes)
            }

    suspend fun checkUpdateAuthorized(
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        jsonLdAttributes: Map<String, Any>,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userCanUpdateEntity(entityId, sub)
            .flatMap {
                checkEntityTypesAreAuthorized(entityTypes)
            }.flatMap {
                checkAttributesAreAuthorized(jsonLdAttributes)
            }

    suspend fun checkUpdateAuthorized(
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        attributeName: ExpandedTerm,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userCanUpdateEntity(entityId, sub)
            .flatMap {
                checkEntityTypesAreAuthorized(entityTypes)
            }.flatMap {
                checkAttributeIsAuthorized(attributeName)
            }

    suspend fun checkUpdateAuthorized(ngsiLdEntity: NgsiLdEntity, sub: Option<Sub>): Either<APIException, Unit> =
        checkUpdateAuthorized(ngsiLdEntity.id, ngsiLdEntity.types, ngsiLdEntity.attributes, sub)

    suspend fun checkReadAuthorized(
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        sub: Option<Sub>
    ): Either<APIException, Unit> =
        userCanReadEntity(entityId, sub).flatMap {
            checkEntityTypesAreAuthorized(entityTypes)
        }
}
