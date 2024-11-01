package com.egm.stellio.search.authorization.web

import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.authorization.service.EntityAccessRightsService
import com.egm.stellio.search.entity.model.NotUpdatedDetails
import com.egm.stellio.search.entity.model.UpdateAttributeResult
import com.egm.stellio.search.entity.model.UpdateOperationResult
import com.egm.stellio.search.entity.model.updateResultFromDetailedResult
import com.egm.stellio.search.entity.util.composeEntitiesQuery
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdRelationship
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.model.toNgsiLdAttribute
import com.egm.stellio.shared.model.toNgsiLdAttributes
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel.ALL_ASSIGNABLE_IAM_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS_TERMS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.ENTITY_REMOVE_OWNERSHIP_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getAuthzContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.parseRepresentations
import com.egm.stellio.shared.util.replaceDefaultContextToAuthzContext
import com.egm.stellio.shared.util.toErrorResponse
import com.egm.stellio.shared.web.BaseHandler
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/entityAccessControl")
class EntityAccessControlHandler(
    private val applicationProperties: ApplicationProperties,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val authorizationService: AuthorizationService
) : BaseHandler() {

    @GetMapping("/entities", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getAuthorizedEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val contexts = getAuthzContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val entitiesQuery = composeEntitiesQuery(
            applicationProperties.pagination,
            params,
            contexts
        ).bind()

        if (!entitiesQuery.attrs.all { ALL_IAM_RIGHTS.contains(it) })
            BadRequestDataException(
                "The attrs parameter only accepts as a value one or more of $ALL_IAM_RIGHTS_TERMS"
            ).left().bind<ResponseEntity<*>>()

        val (count, entities) = authorizationService.getAuthorizedEntities(
            entitiesQuery,
            contexts,
            sub
        ).bind()

        if (count == -1) {
            return@either ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = compactEntities(entities, contexts)

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            count,
            "/ngsi-ld/v1/entityAccessControl/entities",
            entitiesQuery.paginationQuery,
            params,
            mediaType,
            contexts
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

        val contexts = getAuthzContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val entitiesQuery = composeEntitiesQuery(
            applicationProperties.pagination,
            params,
            contexts
        ).bind()

        val (count, entities) =
            authorizationService.getGroupsMemberships(
                entitiesQuery.paginationQuery.offset,
                entitiesQuery.paginationQuery.limit,
                contexts,
                sub
            ).bind()

        if (count == -1) {
            return@either ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = compactEntities(entities, contexts)

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            count,
            "/ngsi-ld/v1/entityAccessControl/groups",
            entitiesQuery.paginationQuery,
            params,
            mediaType,
            contexts
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

        val contexts = getAuthzContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val entitiesQuery = composeEntitiesQuery(
            applicationProperties.pagination,
            params,
            contexts
        ).bind()

        val (count, entities) =
            authorizationService.getUsers(
                entitiesQuery.paginationQuery.offset,
                entitiesQuery.paginationQuery.limit,
                contexts
            ).bind()

        if (count == -1) {
            return@either ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }

        val compactedEntities = compactEntities(entities, contexts)

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            count,
            "/ngsi-ld/v1/entityAccessControl/users",
            entitiesQuery.paginationQuery,
            params,
            mediaType,
            contexts
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
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
            .replaceDefaultContextToAuthzContext(applicationProperties.contexts)
        val expandedAttributes = expandAttributes(body, contexts)
        val ngsiLdAttributes = expandedAttributes.toNgsiLdAttributes().bind()

        // ensure payload contains only relationships and that they are of a known type
        val (validAttributes, invalidAttributes) = ngsiLdAttributes.partition {
            it is NgsiLdRelationship &&
                ALL_ASSIGNABLE_IAM_RIGHTS.contains(it.name)
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

        val isOwnerOfEntity = entityAccessRightsService.isOwnerOfEntity(subjectId, entityId).bind()
        if (!isOwnerOfEntity) {
            entityAccessRightsService.removeRoleOnEntity(subjectId, entityId).bind()
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        } else {
            AccessDeniedException(ENTITY_REMOVE_OWNERSHIP_FORBIDDEN_MESSAGE)
                .left().bind<ResponseEntity<*>>()
        }
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
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
            .replaceDefaultContextToAuthzContext(applicationProperties.contexts)
        val expandedAttribute = expandAttribute(AUTH_TERM_SAP, body.minus(JSONLD_CONTEXT), contexts)
        if (expandedAttribute.first != AUTH_PROP_SAP)
            BadRequestDataException("${expandedAttribute.first} is not authorized property name")
                .left().bind<ResponseEntity<*>>()

        val ngsiLdAttribute = expandedAttribute.toNgsiLdAttribute().bind()

        entityAccessRightsService.updateSpecificAccessPolicy(entityId, ngsiLdAttribute).bind()

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

        entityAccessRightsService.removeSpecificAccessPolicy(entityId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
