package com.egm.stellio.entity.authorization

import arrow.core.Option
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.util.AuthContextModel
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

    fun checkAttributesAreAuthorized(
        ngsiLdAttributes: List<NgsiLdAttribute>,
        entityUri: URI,
        sub: Option<Sub>
    ) = ngsiLdAttributes.forEach { ngsiLdAttribute ->
        checkAttributeIsAuthorized(ngsiLdAttribute.name, entityUri, sub)
    }

    fun checkAttributeIsAuthorized(attributeName: ExpandedTerm, entityUri: URI, sub: Option<Sub>) {
        if (attributeName == AuthContextModel.AUTH_PROP_SAP)
            throw BadRequestDataException(
                "Specific access policy cannot be updated as a normal property, " +
                    "use /ngsi-ld/v1/entityAccessControl/{entityId}/attrs/specificAccessPolicy endpoint instead"
            )
    }
}
