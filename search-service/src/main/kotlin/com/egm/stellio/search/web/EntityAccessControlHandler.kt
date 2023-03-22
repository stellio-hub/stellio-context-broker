package com.egm.stellio.search.web

import arrow.core.*
import arrow.core.continuations.either
import com.egm.stellio.search.authorization.*
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.*
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS_TERMS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import kotlin.collections.flatten

@RestController
@RequestMapping("/ngsi-ld/v1/entityAccessControl")
class EntityAccessControlHandler(
    private val applicationProperties: ApplicationProperties,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val entityPayloadService: EntityPayloadService,
    private val authorizationService: AuthorizationService
) {

    @GetMapping("/entities", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getAuthorizedEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders).bind().addAuthzContextIfNeeded()
        val mediaType = getApplicableMediaType(httpHeaders)

        val queryParams = parseQueryParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        ).bind()

        if (!queryParams.attrs.all { ALL_IAM_RIGHTS.contains(it) })
            BadRequestDataException(
                "The attrs parameter only accepts as a value one or more of $ALL_IAM_RIGHTS_TERMS"
            ).left().bind<ResponseEntity<*>>()

        val countAndAuthorizedEntities = authorizationService.getAuthorizedEntities(
            queryParams,
            contextLink,
            sub
        ).bind()

        if (countAndAuthorizedEntities.first == -1) {
            return@either ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = JsonLdUtils.compactEntities(
            countAndAuthorizedEntities.second,
            queryParams.useSimplifiedRepresentation,
            contextLink,
            mediaType
        )

        buildQueryResponse(
            compactedEntities,
            countAndAuthorizedEntities.first,
            "/ngsi-ld/v1/entityAccessControl/entities",
            queryParams,
            params,
            mediaType,
            contextLink
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @GetMapping("/groups", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getGroupsMemberships(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders).bind().addAuthzContextIfNeeded()
        val mediaType = getApplicableMediaType(httpHeaders)
        val queryParams = parseQueryParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        ).bind()

        val countAndGroupEntities =
            authorizationService.getGroupsMemberships(queryParams.offset, queryParams.limit, sub).bind()

        if (countAndGroupEntities.first == -1) {
            return@either ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = JsonLdUtils.compactEntities(
            countAndGroupEntities.second,
            queryParams.useSimplifiedRepresentation,
            contextLink,
            mediaType
        )

        buildQueryResponse(
            compactedEntities,
            countAndGroupEntities.first,
            "/ngsi-ld/v1/entityAccessControl/groups",
            queryParams,
            params,
            mediaType,
            contextLink
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PostMapping("/{subjectId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addRightsOnEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body).bind().addAuthzContextIfNeeded()
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)

        // ensure payload contains only relationships and that they are of a known type
        val (validAttributes, invalidAttributes) = ngsiLdAttributes.partition {
            it is NgsiLdRelationship &&
                ALL_IAM_RIGHTS.contains(it.name)
        }
        val invalidAttributesDetails = invalidAttributes.map {
            NotUpdatedDetails(it.name, "Not a relationship or not an authorized relationship name")
        }

        val (authorizedInstances, unauthorizedInstances) = validAttributes
            .map { it as NgsiLdRelationship }
            .map { ngsiLdAttribute -> ngsiLdAttribute.getAttributeInstances().map { Pair(ngsiLdAttribute, it) } }
            .flatten()
            .partition {
                // we don't have any sub-relationships here, so let's just take the first
                val targetEntityId = it.second.getLinkedEntitiesIds().first()
                authorizationService.userCanAdminEntity(targetEntityId, sub).isRight()
            }
        val unauthorizedInstancesDetails = unauthorizedInstances.map {
            NotUpdatedDetails(
                it.first.compactName,
                "User is not authorized to manage rights on entity ${it.second.objectId}"
            )
        }

        val results = authorizedInstances.map {
            val (ngsiLdRel, ngsiLdRelInstance) = it
            entityAccessRightsService.setRoleOnEntity(
                subjectId,
                ngsiLdRelInstance.objectId,
                AccessRight.forAttributeName(ngsiLdRel.compactName).orNull()!!
            ).fold(
                ifLeft = { apiException ->
                    UpdateAttributeResult(
                        ngsiLdRel.compactName,
                        ngsiLdRelInstance.datasetId,
                        UpdateOperationResult.FAILED,
                        apiException.message
                    )
                },
                ifRight = {
                    UpdateAttributeResult(
                        ngsiLdRel.compactName,
                        ngsiLdRelInstance.datasetId,
                        UpdateOperationResult.APPENDED,
                        null
                    )
                }
            )
        }
        val appendResult = updateResultFromDetailedResult(results)

        if (invalidAttributes.isEmpty() && unauthorizedInstances.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else {
            val fullAppendResult = appendResult.copy(
                updated = appendResult.updated,
                notUpdated = appendResult.notUpdated.plus(invalidAttributesDetails)
                    .plus(unauthorizedInstancesDetails)
            )
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(fullAppendResult)
        }
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/{subjectId}/attrs/{entityId}")
    suspend fun removeRightsOnEntity(
        @PathVariable subjectId: String,
        @PathVariable entityId: String
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val entityUri = entityId.toUri()

        authorizationService.userCanAdminEntity(entityUri, sub).bind()

        entityAccessRightsService.removeRoleOnEntity(subjectId, entityUri).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PostMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun updateSpecificAccessPolicy(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val entityUri = entityId.toUri()
        authorizationService.userCanAdminEntity(entityUri, sub).bind()

        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body).bind().addAuthzContextIfNeeded()
        val rawPayload = mapOf(AUTH_TERM_SAP to JsonLdUtils.removeContextFromInput(body))
        val expandedPayload = expandJsonLdFragment(rawPayload, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(expandedPayload)
        if (ngsiLdAttributes[0].name != AUTH_PROP_SAP)
            BadRequestDataException(
                "${ngsiLdAttributes[0].name} is not authorized property name"
            ).left().bind<ResponseEntity<*>>()

        entityPayloadService.updateSpecificAccessPolicy(entityUri, ngsiLdAttributes[0]).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun deleteSpecificAccessPolicy(
        @PathVariable entityId: String
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val entityUri = entityId.toUri()
        authorizationService.userCanAdminEntity(entityUri, sub).bind()

        entityPayloadService.removeSpecificAccessPolicy(entityUri).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
