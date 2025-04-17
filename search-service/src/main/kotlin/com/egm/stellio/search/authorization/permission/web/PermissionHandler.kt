package com.egm.stellio.search.authorization.permission.web

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.CHANGE_OWNER_EXCEPTION
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.EVERYONE_AS_ADMIN_EXCEPTION
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.deserialize
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.unauthorizedCreateMessage
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.unauthorizedEditMessage
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.unauthorizedRetrieveMessage
import com.egm.stellio.search.authorization.permission.model.PermissionFilters
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.authorization.permission.service.PermissionService
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.PaginationQuery.Companion.parsePaginationParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.AuthContextModel.AUTH_ACTION_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_ASSIGNEE_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_ASSIGNER_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TARGET_TERM
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getAuthzContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.getAuthzContextFromRequestOrDefault
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.parseAndExpandQueryParameter
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.util.toStringValue
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.BaseHandler
import com.fasterxml.jackson.module.kotlin.convertValue
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
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
@RequestMapping("/ngsi-ld/v1/auth/permissions")
@Validated
class PermissionHandler(
    private val applicationProperties: ApplicationProperties,
    private val permissionService: PermissionService,
    private val subjectReferentialService: SubjectReferentialService,
    private val entityQueryService: EntityQueryService,
    private val authorizationService: AuthorizationService
) : BaseHandler() {

    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = getAuthzContextFromRequestOrDefault(httpHeaders, body, applicationProperties.contexts).bind()
        val sub = getSubFromSecurityContext()

        val permission = deserialize(body, contexts).bind().copy(assigner = sub.toStringValue())
        checkIsAdmin(permission).bind()

        if (permission.action == Action.OWN) {
            CHANGE_OWNER_EXCEPTION
                .left().bind<APIException>()
        }

        if (permission.action == Action.ADMIN && permission.assignee == null) {
            EVERYONE_AS_ADMIN_EXCEPTION.left()
                .bind<APIException>()
        }

        permissionService.create(permission).bind()

        ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/auth/permissions/${permission.id}"))
            .build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun query(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(
            implemented = [
                QP.OPTIONS, QP.COUNT, QP.OFFSET, QP.LIMIT,
                QP.ACTION, QP.ASSIGNEE, QP.ASSIGNER,
                QP.ID, QP.DETAILS, QP.DETAILS_PICK
            ],
            notImplemented = [
                QP.TYPE, QP.SCOPEQ
            ]
        )
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val contexts = getAuthzContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val permissionFilters = PermissionFilters.fromQueryParameters(queryParams, contexts).bind()
        val includeSysAttrs = queryParams.getOrDefault(QP.OPTIONS.key, emptyList())
            .contains(OptionsValue.SYS_ATTRS.value)
        val includeDetails = queryParams.getFirst(QP.DETAILS.key)?.toBoolean() ?: false
        val pickDetailsAttributes = parseAndExpandQueryParameter(
            queryParams.getFirst(QueryParameter.DETAILS_PICK.key),
            contexts
        )
        val paginationQuery = parsePaginationParameters(
            queryParams,
            applicationProperties.pagination.limitDefault,
            applicationProperties.pagination.limitMax
        ).bind()
        val permissions = serializePermissions(
            permissionService.getPermissions(
                permissionFilters,
                paginationQuery.limit,
                paginationQuery.offset,
            ).bind(),
            contexts,
            mediaType,
            includeSysAttrs,
            includeDetails,
            pickDetailsAttributes
        ).bind()
        val permissionsCount = permissionService.getPermissionsCount(
            permissionFilters
        ).bind()

        buildQueryResponse(
            permissions,
            permissionsCount,
            "/ngsi-ld/v1/auth/permissions",
            paginationQuery,
            queryParams,
            mediaType,
            contexts
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @GetMapping("/{permissionId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun retrieve(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable permissionId: URI,
        @AllowedParameters(implemented = [QP.OPTIONS, QP.DETAILS, QP.DETAILS_PICK])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val options = queryParams.getFirst(QP.OPTIONS.key)
        val includeSysAttrs = options?.contains(OptionsValue.SYS_ATTRS.value) ?: false
        val includeDetails = queryParams.getFirst(QP.DETAILS.key)?.toBoolean() ?: false
        val contexts = getAuthzContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts).bind()
        val pickDetailsAttributes = parseAndExpandQueryParameter(
            queryParams.getFirst(QueryParameter.DETAILS_PICK.key),
            contexts
        )
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val subjects = subjectReferentialService.getSubjectAndGroupsUUID().bind()

        val permission = permissionService.getById(permissionId).bind()

        if (permission.assignee !in subjects && checkIsAdmin(permissionId).isLeft()) {
            AccessDeniedException(unauthorizedRetrieveMessage(permissionId)).left().bind<String>()
        }

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(
                serializePermission(
                    permission,
                    contexts,
                    mediaType,
                    includeSysAttrs,
                    includeDetails,
                    pickDetailsAttributes
                ).bind()
            )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping(
        "/{permissionId}",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun update(
        @PathVariable permissionId: URI,
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters // no query parameter is allowed
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        checkIsAdmin(permissionId).bind()
        val currentPermission = permissionService.getById(permissionId).bind()

        if (currentPermission.action == Action.OWN) {
            CHANGE_OWNER_EXCEPTION.left().bind<APIException>()
        }

        val body = requestBody.awaitFirst().deserializeAsMap()

        if (body[AUTH_ACTION_TERM] == Action.OWN.value) {
            CHANGE_OWNER_EXCEPTION.left().bind<APIException>()
        }

        val newAssigneeIsEveryone =
            !body.containsKey(AUTH_ASSIGNEE_TERM) && currentPermission.assignee == null ||
                body.containsKey(AUTH_ASSIGNEE_TERM) && body[AUTH_ASSIGNEE_TERM] == null
        val newActionIsAdmin = !body.containsKey(AUTH_ACTION_TERM) && currentPermission.action == Action.ADMIN ||
            body[AUTH_ACTION_TERM] == Action.ADMIN.value
        if (newActionIsAdmin && newAssigneeIsEveryone) {
            EVERYONE_AS_ADMIN_EXCEPTION.left().bind<APIException>()
        }

        body[AUTH_TARGET_TERM]?.let {
            val target = it as Map<String, Any>
            target[NGSILD_ID_TERM]?.let { entityId ->
                val entityUri = (entityId as String).toUri()
                authorizationService.userCanAdminEntity(entityUri, getSubFromSecurityContext()).bind()
            }
        }

        val contexts = getAuthzContextFromRequestOrDefault(httpHeaders, body, applicationProperties.contexts).bind()
        permissionService.update(permissionId, body, contexts).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/{permissionId}")
    suspend fun delete(
        @PathVariable permissionId: URI,
        @AllowedParameters // no query parameter is allowed
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        checkIsAdmin(permissionId).bind()

        permissionService.delete(permissionId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    private suspend fun checkIsAdmin(permissionId: URI): Either<APIException, Unit> =
        permissionService.isAdminOf(permissionId, getSubFromSecurityContext())
            .flatMap {
                if (!it)
                    AccessDeniedException(
                        unauthorizedEditMessage(permissionId)
                    ).left()
                else
                    Unit.right()
            }

    private suspend fun checkIsAdmin(permission: Permission): Either<APIException, Unit> =
        permissionService.checkHasPermissionOnEntity(getSubFromSecurityContext(), permission.target.id, Action.ADMIN)
            .flatMap {
                if (!it)
                    AccessDeniedException(
                        unauthorizedCreateMessage(permission.target.id)
                    ).left()
                else
                    Unit.right()
            }

    private suspend fun serializePermission(
        permission: Permission,
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false,
        includeDetails: Boolean = false,
        pickAttributes: Set<String> = emptySet()
    ): Either<APIException, String> = either {
        val permissionMap = DataTypes.mapper.convertValue<Map<String, Any>>(permission.compact(contexts)).plus(
            JSONLD_CONTEXT_KW to contexts
        ).let { DataTypes.toFinalRepresentation(it, mediaType, includeSysAttrs) }.toMutableMap()

        if (includeDetails) {
            permission.assignee?.let { assignee ->
                permissionMap[AUTH_ASSIGNEE_TERM] = subjectReferentialService.retrieve(assignee)
                    .bind().getSubjectInfoValue()
            }
            permission.assigner?.let { assigner ->
                permissionMap[AUTH_ASSIGNER_TERM] = subjectReferentialService.retrieve(assigner)
                    .bind().getSubjectInfoValue()
            }
            permission.target.id.let { id ->
                permissionMap[AUTH_TARGET_TERM] = compactEntity(
                    entityQueryService.queryEntity(id, getSubFromSecurityContext().getOrNull()).bind()
                        .filterAttributes(pickAttributes, emptySet()),
                    contexts
                ).minus(JSONLD_CONTEXT_KW)
            }
        }

        DataTypes.mapper.writeValueAsString(permissionMap)
    }

    private suspend fun serializePermissions(
        permissions: List<Permission>,
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean,
        includeDetails: Boolean,
        pickDetailsAttributes: Set<String> = emptySet()
    ): Either<APIException, String> = either {
        permissions.map {
            serializePermission(it, contexts, mediaType, includeSysAttrs, includeDetails, pickDetailsAttributes).bind()
        }.toString()
    }
}
