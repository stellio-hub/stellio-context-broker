package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.model.NotUpdatedDetails
import com.egm.stellio.entity.model.updateResultFromDetailedResult
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.NgsiLdRelationship
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/entityAccessControl")
class EntityAccessControlHandler(
    private val entityService: EntityService,
    private val authorizationService: AuthorizationService,
    private val entityEventService: EntityEventService
) {

    @PostMapping("/{subjectId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addRightsOnEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = JsonLdUtils.expandJsonLdFragment(body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)

        // ensure payload contains only relationships and that they are of a known type
        val (validAttributes, invalidAttributes) = ngsiLdAttributes.partition {
            it is NgsiLdRelationship &&
                AuthorizationService.IAM_RIGHTS.contains(it.name)
        }
        val invalidAttributesDetails = invalidAttributes.map {
            NotUpdatedDetails(it.compactName, "Not a relationship or not an authorized relationship name")
        }

        val (authorizedInstances, unauthorizedInstances) = validAttributes
            .map { it as NgsiLdRelationship }
            .map { ngsiLdAttribute -> ngsiLdAttribute.getAttributeInstances().map { Pair(ngsiLdAttribute, it) } }
            .flatten()
            .partition {
                // we don't have any sub-relationships here, so let's just take the first
                val targetEntityId = it.second.getLinkedEntitiesIds().first()
                authorizationService.userIsAdminOfEntity(targetEntityId, userId)
            }
        val unauthorizedInstancesDetails = unauthorizedInstances.map {
            NotUpdatedDetails(
                it.first.compactName,
                "User is not authorized to manage rights on entity ${it.second.objectId}"
            )
        }

        val results = authorizedInstances.map {
            entityService.appendEntityRelationship(
                subjectId.toUri(),
                it.first,
                it.second,
                false
            )
        }
        val appendResult = updateResultFromDetailedResult(results)

        if (appendResult.updated.isNotEmpty())
            entityEventService.publishAttributeAppendEvents(
                subjectId.toUri(),
                jsonLdAttributes,
                appendResult,
                contexts
            )

        return if (invalidAttributes.isEmpty() && unauthorizedInstances.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else {
            val fullAppendResult = appendResult.copy(
                notUpdated = appendResult.notUpdated.plus(invalidAttributesDetails).plus(unauthorizedInstancesDetails)
            )
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(fullAppendResult)
        }
    }

    @DeleteMapping("/{subjectId}/attrs/{entityId}")
    suspend fun removeRightsOnEntity(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders))

        if (!authorizationService.userIsAdminOfEntity(entityId.toUri(), userId))
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("User is not authorized to manage rights on entity $entityId")

        val removeResult =
            authorizationService.removeUserRightsOnEntity(entityId.toUri(), subjectId.toUri())
                .also {
                    if (it != 0)
                        entityEventService.publishAttributeDeleteEvent(
                            entityId = subjectId.toUri(),
                            attributeName = entityId,
                            deleteAll = false,
                            contexts = contexts
                        )
                }

        return if (removeResult != 0)
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            throw ResourceNotFoundException("Subject $subjectId has no right on entity $entityId")
    }
}
