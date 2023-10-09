package com.egm.stellio.search.web

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.authorization.*
import com.egm.stellio.search.model.*
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS_TERMS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonLdUtils.removeContextFromInput
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.web.BaseHandler
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.net.URI
import kotlin.collections.flatten

@RestController
@RequestMapping("/ngsi-ld/v1/entityAccessControl")
class EntityAccessControlHandler(
    private val applicationProperties: ApplicationProperties,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val entityPayloadService: EntityPayloadService,
    private val authorizationService: AuthorizationService
) : BaseHandler() {

    @GetMapping("/entities", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getAuthorizedEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val contextLink = getAuthzContextFromLinkHeaderOrDefault(httpHeaders).bind()
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

        val contextLink = getAuthzContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders)
        val queryParams = parseQueryParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        ).bind()

        val countAndGroupEntities =
            authorizationService.getGroupsMemberships(queryParams.offset, queryParams.limit, contextLink, sub).bind()

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

    @GetMapping("/users", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getUsers(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        authorizationService.userIsAdmin(sub).bind()

        val contextLink = getAuthzContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders)
        val queryParams = parseQueryParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        ).bind()

        val countAndUserEntities =
            authorizationService.getUsers(queryParams.offset, queryParams.limit, contextLink).bind()

        if (countAndUserEntities.first == -1) {
            return@either ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = JsonLdUtils.compactEntities(
            countAndUserEntities.second,
            queryParams.useSimplifiedRepresentation,
            contextLink,
            mediaType
        )

        buildQueryResponse(
            compactedEntities,
            countAndUserEntities.first,
            "/ngsi-ld/v1/entityAccessControl/users",
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
        val contexts = checkAndGetContext(httpHeaders, body).bind().replaceDefaultContextToAuthzContext()
        val expandedAttributes = expandAttributes(body, contexts)
        val ngsiLdAttributes = expandedAttributes.toNgsiLdAttributes().bind()

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
                authorizationService.userCanAdminEntity(it.second.objectId, sub).isRight()
            }
        val unauthorizedInstancesDetails = unauthorizedInstances.map {
            NotUpdatedDetails(
                it.first.name,
                "User is not authorized to manage rights on entity ${it.second.objectId}"
            )
        }

        val results = authorizedInstances.map {
            val (ngsiLdRel, ngsiLdRelInstance) = it
            entityAccessRightsService.setRoleOnEntity(
                subjectId,
                ngsiLdRelInstance.objectId,
                AccessRight.forAttributeName(ngsiLdRel.name).getOrNull()!!
            ).fold(
                ifLeft = { apiException ->
                    UpdateAttributeResult(
                        ngsiLdRel.name,
                        ngsiLdRelInstance.datasetId,
                        UpdateOperationResult.FAILED,
                        apiException.message
                    )
                },
                ifRight = {
                    UpdateAttributeResult(
                        ngsiLdRel.name,
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
        @PathVariable entityId: URI
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        authorizationService.userCanAdminEntity(entityId, sub).bind()

        entityAccessRightsService.removeRoleOnEntity(subjectId, entityId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PostMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun updateSpecificAccessPolicy(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        authorizationService.userCanAdminEntity(entityId, sub).bind()

        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body).bind().replaceDefaultContextToAuthzContext()
        val expandedAttribute = expandAttribute(AUTH_TERM_SAP, removeContextFromInput(body), contexts)
        if (expandedAttribute.first != AUTH_PROP_SAP)
            BadRequestDataException("${expandedAttribute.first} is not authorized property name")
                .left().bind<ResponseEntity<*>>()

        val ngsiLdAttribute = expandedAttribute.toNgsiLdAttribute().bind()

        entityPayloadService.updateSpecificAccessPolicy(entityId, ngsiLdAttribute).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/{entityId}/attrs/specificAccessPolicy")
    suspend fun deleteSpecificAccessPolicy(
        @PathVariable entityId: URI
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        authorizationService.userCanAdminEntity(entityId, sub).bind()

        entityPayloadService.removeSpecificAccessPolicy(entityId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
