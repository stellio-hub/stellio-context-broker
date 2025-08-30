package com.egm.stellio.search.authorization.permission.web

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.CREATE_OR_UPDATE_OWN_EXCEPTION
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.DELETE_OWN_EXCEPTION
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.EVERYONE_AS_ADMIN_EXCEPTION
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.deserialize
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.unauthorizedRetrieveMessage
import com.egm.stellio.search.authorization.permission.model.PermissionFilters
import com.egm.stellio.search.authorization.permission.model.PermissionFilters.Companion.PermissionKind
import com.egm.stellio.search.authorization.permission.service.PermissionService
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.PaginationQuery.Companion.parsePaginationParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.AuthContextModel.AUTH_ASSIGNEE_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_ASSIGNER_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TARGET_TERM
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SUBJECT_ID
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
import com.egm.stellio.shared.util.parseAndExpandPickOmitParameters
import com.egm.stellio.shared.util.parseAndExpandQueryParameter
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.web.BaseHandler
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
    private val entityQueryService: EntityQueryService
) : BaseHandler() {

    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = getAuthzContextFromRequestOrDefault(httpHeaders, body, applicationProperties.contexts).bind()
        val sub = getSubFromSecurityContext()

        val permission = deserialize(body, contexts).bind().copy(assigner = sub.orEmpty())
        checkCanCreateOrUpdate(permission).bind()

        permissionService.create(permission).bind()

        ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/auth/permissions/${permission.id}"))
            .build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun queryOnlyAdmin(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(
            implemented = [
                QP.OPTIONS, QP.COUNT, QP.OFFSET, QP.LIMIT,
                QP.ACTION, QP.ASSIGNEE, QP.ASSIGNER,
                QP.TARGET_ID, QP.DETAILS, QP.DETAILS_PICK,
                QP.TARGET_TYPE,
            ],
            notImplemented = [
                QP.TARGET_SCOPEQ
            ]
        )
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = query(httpHeaders, queryParams, PermissionKind.ADMIN)

    @GetMapping(path = [ "/assigned"], produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun queryOnlyAssigned(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(
            implemented = [
                QP.OPTIONS, QP.COUNT, QP.OFFSET, QP.LIMIT,
                QP.ACTION, QP.ASSIGNEE, QP.ASSIGNER,
                QP.TARGET_ID, QP.DETAILS, QP.DETAILS_PICK,
                QP.TARGET_TYPE, QP.TARGET_SCOPEQ
            ],
            notImplemented = []
        )
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = query(httpHeaders, queryParams, PermissionKind.ASSIGNED)

    suspend fun query(
        httpHeaders: HttpHeaders,
        queryParams: MultiValueMap<String, String>,
        kind: PermissionKind
    ) = either {
        val contexts = getAuthzContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val permissionFilters = PermissionFilters.fromQueryParameters(
            queryParams,
            contexts,
            kind
        ).bind()
        val includeSysAttrs = queryParams.getOrDefault(QP.OPTIONS.key, emptyList())
            .contains(OptionsValue.SYS_ATTRS.value)
        val includeDetails = queryParams.getFirst(QP.DETAILS.key)?.toBoolean() ?: false
        val pickDetailsAttributes = parseAndExpandPickOmitParameters(
            queryParams.getFirst(QueryParameter.DETAILS_PICK.key),
            null,
            contexts
        ).bind().first
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

        if (permission.assignee !in subjects &&
            permissionService.hasPermissionOnTarget(permission.target, Action.ADMIN).isLeft()
        ) {
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
        val currentPermission = permissionService.getById(permissionId).bind()
        permissionService.hasPermissionOnTarget(currentPermission.target, Action.ADMIN).bind()

        if (currentPermission.action == Action.OWN) {
            CREATE_OR_UPDATE_OWN_EXCEPTION.left().bind<APIException>()
        }

        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = getAuthzContextFromRequestOrDefault(httpHeaders, body, applicationProperties.contexts).bind()

        val permission = currentPermission.mergeWithFragment(body, contexts).bind()
        checkCanCreateOrUpdate(permission).bind()

        permissionService.upsert(permission).bind()

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
        val currentPermission = permissionService.getById(permissionId).bind()
        checkCanDelete(currentPermission).bind()

        if (currentPermission.action == Action.OWN) {
            DELETE_OWN_EXCEPTION.left().bind<APIException>()
        }

        permissionService.delete(permissionId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    private suspend fun checkCanCreateOrUpdate(permission: Permission): Either<APIException, Unit> = either {
        permissionService.hasPermissionOnTarget(permission.target, Action.ADMIN).bind()

        if (permission.action == Action.OWN) {
            CREATE_OR_UPDATE_OWN_EXCEPTION.left().bind<APIException>()
        }

        if (permission.action == Action.ADMIN && permission.assignee == null) {
            EVERYONE_AS_ADMIN_EXCEPTION.left().bind<APIException>()
        }

        permission.assignee?.let { subjectReferentialService.getSubjectAndGroupsUUID(it).bind() }
        permission.target.id?.let {
            entityQueryService.checkEntityExistence(it, excludeDeleted = false).bind()
        }
    }

    private suspend fun checkCanDelete(permission: Permission): Either<APIException, Unit> =
        permissionService.hasPermissionOnTarget(
            permission.target,
            Action.ADMIN
        )

    private suspend fun serializePermission(
        expandedPermission: Permission,
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false,
        includeDetails: Boolean = false,
        pickAttributes: Set<String> = emptySet()
    ): Either<APIException, String> = either {
        val permission = expandedPermission.compact(contexts)
        val permissionMap = DataTypes.convertTo<Map<String, Any>>(permission.compact(contexts)).plus(
            JSONLD_CONTEXT_KW to contexts
        ).let { DataTypes.toFinalRepresentation(it, mediaType, includeSysAttrs) }.toMutableMap()

        if (includeDetails) {
            permission.assignee?.let { assignee ->
                permissionMap[AUTH_ASSIGNEE_TERM] = subjectReferentialService.retrieve(assignee)
                    .fold(
                        { mapOf(AUTH_TERM_SUBJECT_ID to assignee) },
                        { it.toSerializableMap() }
                    )
            }
            permission.assigner?.let { assigner ->
                permissionMap[AUTH_ASSIGNER_TERM] = subjectReferentialService.retrieve(assigner)
                    .fold(
                        { mapOf(AUTH_TERM_SUBJECT_ID to assigner) },
                        { it.toSerializableMap() }
                    )
            }
            permission.target.id?.let { id ->
                permissionMap[AUTH_TARGET_TERM] = compactEntity(
                    entityQueryService.queryEntity(id, excludeDeleted = false).bind()
                        .filterPickAndOmit(pickAttributes, emptySet()),
                    contexts
                ).minus(JSONLD_CONTEXT_KW)
            }
        }

        DataTypes.serialize(permissionMap)
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
