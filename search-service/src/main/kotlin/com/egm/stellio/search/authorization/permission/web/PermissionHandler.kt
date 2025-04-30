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
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.unauthorizedMessage
import com.egm.stellio.search.authorization.permission.model.PermissionFilters
import com.egm.stellio.search.authorization.permission.model.serialize
import com.egm.stellio.search.authorization.permission.service.PermissionService
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.PaginationQuery.Companion.parsePaginationParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.isURI
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.util.toStringValue
import com.egm.stellio.shared.util.toUri
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
@RequestMapping("/ngsi-ld/v1/auth")
@Validated
class PermissionHandler(
    private val applicationProperties: ApplicationProperties,
    private val permissionService: PermissionService,
    private val subjectReferentialService: SubjectReferentialService
) : BaseHandler() {

    @PostMapping(
        path = ["permissionOperations/create"],
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE]
    )
    suspend fun batchCreate(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val body = requestBody.awaitFirst().deserializeAsList()
        val operationResult = BatchOperationResult()

        body.map { permissionBody -> // can't do proper error handling if it is missing some id.
            val id = permissionBody["id"] as? String
            val asValidId = id?.isURI() ?: false
            if (asValidId)
                return BadRequestDataException("batch creation does not support null id").toErrorResponse()

            id!!.toUri() to permissionBody
        }.forEach { (id, permissionBody) ->
            val creationResponse = either {
                val contexts = checkAndGetContext(httpHeaders, permissionBody, applicationProperties.contexts.core)
                    .bind()
                val permission = deserialize(permissionBody, contexts).bind()
                permissionService.create(permission).bind()
            }

            operationResult.addEither(creationResponse, id)
        }

        return operationResult.toEndpointResponse()
    }

    @PostMapping(path = ["permissions"], consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
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

    @GetMapping(path = ["permissions"], produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun query(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(
            implemented = [
                QP.OPTIONS, QP.COUNT, QP.OFFSET, QP.LIMIT,
                QP.ACTION, QP.ASSIGNEE, QP.ASSIGNER,
                QP.ID
            ],
            notImplemented = [
                QP.JOIN, QP.JOIN_LEVEL,
                QP.TYPE, QP.SCOPEQ
            ]
        )
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val permissionFilters = PermissionFilters.fromQueryParameters(queryParams, contexts).bind()

        val includeSysAttrs = queryParams.getOrDefault(QP.OPTIONS.key, emptyList())
            .contains(OptionsValue.SYS_ATTRS.value)
        val paginationQuery = parsePaginationParameters(
            queryParams,
            applicationProperties.pagination.limitDefault,
            applicationProperties.pagination.limitMax
        ).bind()
        val permissions = permissionService.getPermissions(
            permissionFilters,
            paginationQuery.limit,
            paginationQuery.offset,
        ).bind().serialize(contexts, mediaType, includeSysAttrs)
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

    @GetMapping(
        "permissionspermissions/{permissionId}",
        produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE]
    )
    suspend fun retrieve(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable permissionId: URI,
        @AllowedParameters(implemented = [QP.OPTIONS])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val options = queryParams.getFirst(QP.OPTIONS.key)
        val includeSysAttrs = options?.contains(OptionsValue.SYS_ATTRS.value) ?: false
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val subjects = subjectReferentialService.getSubjectAndGroupsUUID().bind()

        val permission = permissionService.getById(permissionId).bind()

        if (permission.assignee !in subjects) {
            checkIsAdmin(permissionId).bind()
        }

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(permission.serialize(contexts, mediaType, includeSysAttrs))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping(
        "permissions/{permissionId}",
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
        val oldPermission = permissionService.getById(permissionId).bind()

        if (oldPermission.action == Action.OWN) {
            CHANGE_OWNER_EXCEPTION.left().bind<APIException>()
        }

        val body = requestBody.awaitFirst().deserializeAsMap()

        if (body["action"] == Action.OWN.value) {
            CHANGE_OWNER_EXCEPTION.left().bind<APIException>()
        }
        if (body["assignee"] == null &&
            (oldPermission.action == Action.ADMIN || body["action"] == Action.ADMIN.value)
        ) {
            EVERYONE_AS_ADMIN_EXCEPTION.left().bind<APIException>()
        }

        val contexts = checkAndGetContext(httpHeaders, body, applicationProperties.contexts.core).bind()
        permissionService.update(permissionId, body, contexts).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("permissions/{permissionId}")
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
                        unauthorizedMessage(permissionId)
                    ).left()
                else
                    Unit.right()
            }

    private suspend fun checkIsAdmin(permission: Permission): Either<APIException, Unit> =
        permissionService.checkHasPermissionOnEntity(getSubFromSecurityContext(), permission.target.id, Action.ADMIN)
            .flatMap {
                if (!it)
                    AccessDeniedException(
                        unauthorizedMessage(permission.target.id)
                    ).left()
                else
                    Unit.right()
            }
}
