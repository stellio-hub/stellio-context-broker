package com.egm.stellio.search.web

import arrow.core.*
import arrow.core.continuations.either
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.*
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS_TERMS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.COMPOUND_AUTHZ_CONTEXT
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
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        val queryParams = parseAndCheckParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        )

        if (queryParams.q != null && !ALL_IAM_RIGHTS_TERMS.contains(queryParams.q))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(
                    BadRequestDataResponse(
                        "The parameter q only accepts as a value one or more of $ALL_IAM_RIGHTS_TERMS"
                    )
                )

        val countAndAuthorizedEntities = authorizationService.getAuthorizedEntities(
            queryParams,
            contextLink,
            sub,
        )

        if (countAndAuthorizedEntities.first == -1) {
            return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = JsonLdUtils.compactEntities(
            countAndAuthorizedEntities.second,
            queryParams.useSimplifiedRepresentation,
            contextLink,
            mediaType
        )

        return buildQueryResponse(
            compactedEntities,
            countAndAuthorizedEntities.first,
            "/ngsi-ld/v1/entityAccessControl/entities",
            queryParams,
            params,
            mediaType,
            contextLink
        )
    }

    @GetMapping("/groups", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getGroupsMemberships(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
            val mediaType = getApplicableMediaType(httpHeaders)
            val queryParams = parseAndCheckParams(
                Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
                params,
                contextLink
            )

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
    }

    @PostMapping("/{subjectId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addRightsOnEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable subjectId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val body = requestBody.awaitFirst().deserializeAsMap()
            val contexts = checkAndGetContext(httpHeaders, body)
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
                    authorizationService.userIsAdminOfEntity(targetEntityId, sub).isRight()
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
    }

    @DeleteMapping("/{subjectId}/attrs/{entityId}")
    suspend fun removeRightsOnEntity(
        @PathVariable subjectId: String,
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            authorizationService.userIsAdminOfEntity(entityId.toUri(), sub).bind()

            entityAccessRightsService.removeRoleOnEntity(subjectId, entityId.toUri()).bind()

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    @PostMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun updateSpecificAccessPolicy(
        @PathVariable entityId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val entityUri = entityId.toUri()
            authorizationService.userIsAdminOfEntity(entityUri, sub).bind()

            val body = requestBody.awaitFirst()
            val expandedPayload = expandJsonLdFragment(AUTH_TERM_SAP, body, COMPOUND_AUTHZ_CONTEXT)
            val ngsiLdAttributes = parseToNgsiLdAttributes(expandedPayload)

            entityPayloadService.updateSpecificAccessPolicy(entityUri, ngsiLdAttributes[0]).bind()

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    @DeleteMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun deleteSpecificAccessPolicy(
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val entityUri = entityId.toUri()
            authorizationService.userIsAdminOfEntity(entityUri, sub).bind()

            entityPayloadService.removeSpecificAccessPolicy(entityId.toUri()).bind()

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }
}
