package com.egm.stellio.search.web

import arrow.core.Either
import arrow.core.Option
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityOperationService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntities
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
class EntityOperationHandler(
    private val entityOperationService: EntityOperationService,
    private val entityPayloadService: EntityPayloadService,
    private val authorizationService: AuthorizationService,
    private val entityEventService: EntityEventService
) {

    /**
     * Implements 6.14.3.1 - Create Batch of Entities
     */
    @PostMapping("/create", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val body = requestBody.awaitFirst().deserializeAsList()
            .checkNamesAreNgsiLdSupported().bind()
            .checkContentIsNgsiLdSupported().bind()
        checkBatchRequestBody(body).bind()
        checkContext(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()
        val (expandedEntities, ngsiLdEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType).bind()
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)
        val (unauthorizedEntities, authorizedEntities) = newEntities.partition {
            authorizationService.userCanCreateEntities(sub).isLeft()
        }
        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(existingEntities, ENTITY_ALREADY_EXISTS_MESSAGE)
            addEntitiesToErrors(unauthorizedEntities, ENTITIY_CREATION_FORBIDDEN_MESSAGE)
        }

        doBatchCreation(authorizedEntities, expandedEntities, batchOperationResult, sub)

        if (batchOperationResult.errors.isEmpty())
            ResponseEntity.status(HttpStatus.CREATED).body(batchOperationResult.getSuccessfulEntitiesIds())
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.15.3.1 - Upsert Batch of Entities
     */
    @PostMapping("/upsert", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun upsert(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @RequestParam(required = false) options: String?
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val body = requestBody.awaitFirst().deserializeAsList()
            .checkNamesAreNgsiLdSupported().bind()
            .checkContentIsNgsiLdSupported().bind()
        checkBatchRequestBody(body).bind()
        checkContext(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()

        val (expandedEntities, ngsiLdEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType).bind()
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

        val (newUnauthorizedEntities, newAuthorizedEntities) = newEntities.partition {
            authorizationService.userCanCreateEntities(sub).isLeft()
        }
        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(newUnauthorizedEntities, ENTITIY_CREATION_FORBIDDEN_MESSAGE)
        }

        doBatchCreation(newAuthorizedEntities, expandedEntities, batchOperationResult, sub)

        val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
            existingEntities.partition { authorizationService.userCanUpdateEntity(it.id, sub).isLeft() }
        batchOperationResult.addEntitiesToErrors(existingEntitiesUnauthorized, ENTITY_UPDATE_FORBIDDEN_MESSAGE)

        if (existingEntitiesAuthorized.isNotEmpty()) {
            val entitiesToUpdate = existingEntitiesAuthorized.map { ngsiLdEntity ->
                Pair(ngsiLdEntity, expandedEntities.find { ngsiLdEntity.id.toString() == it.id }!!)
            }
            val updateOperationResult = when (options) {
                "update" -> entityOperationService.update(entitiesToUpdate, false, sub.getOrNull())
                else -> entityOperationService.replace(entitiesToUpdate, sub.getOrNull())
            }

            if (options == "update")
                publishUpdateEvents(sub.getOrNull(), updateOperationResult, expandedEntities, ngsiLdEntities)
            else
                publishReplaceEvents(sub.getOrNull(), updateOperationResult, ngsiLdEntities)

            batchOperationResult.errors.addAll(updateOperationResult.errors)
            batchOperationResult.success.addAll(updateOperationResult.success)
        }

        if (batchOperationResult.errors.isEmpty() && newEntities.isNotEmpty())
            ResponseEntity.status(HttpStatus.CREATED).body(newEntities.map { it.id })
        else if (batchOperationResult.errors.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.16.3.1 - Update Batch of Entities
     */
    @PostMapping("/update", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun update(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @RequestParam options: Optional<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val body = requestBody.awaitFirst().deserializeAsList()
            .checkNamesAreNgsiLdSupported().bind()
            .checkContentIsNgsiLdSupported().bind()
        checkBatchRequestBody(body).bind()
        checkContext(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()
        val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)

        val (expandedEntities, ngsiLdEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType).bind()
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

        val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
            existingEntities.partition { authorizationService.userCanUpdateEntity(it.id, sub).isLeft() }

        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(newEntities, ENTITY_DOES_NOT_EXIST_MESSAGE)
            addEntitiesToErrors(existingEntitiesUnauthorized, ENTITY_UPDATE_FORBIDDEN_MESSAGE)
        }

        if (existingEntitiesAuthorized.isNotEmpty()) {
            val entitiesToUpdate = existingEntitiesAuthorized.map { ngsiLdEntity ->
                Pair(ngsiLdEntity, expandedEntities.find { ngsiLdEntity.id.toString() == it.id }!!)
            }
            val updateOperationResult =
                entityOperationService.update(entitiesToUpdate, disallowOverwrite, sub.getOrNull())

            publishUpdateEvents(sub.getOrNull(), updateOperationResult, expandedEntities, ngsiLdEntities)

            batchOperationResult.errors.addAll(updateOperationResult.errors)
            batchOperationResult.success.addAll(updateOperationResult.success)
        }

        if (batchOperationResult.errors.isEmpty() && newEntities.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.17.3.1 - Delete Batch of Entities
     */
    @PostMapping("/delete", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun delete(@RequestBody requestBody: Mono<List<String>>): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val body = requestBody.awaitFirst()
        checkBatchRequestBody(body).bind()

        val (existingEntities, unknownEntities) =
            entityOperationService.splitEntitiesIdsByExistence(body.toListOfUri())

        val entitiesIdsToDelete = existingEntities.toSet()
        val entitiesBeforeDelete =
            if (entitiesIdsToDelete.isNotEmpty())
                entityPayloadService.retrieve(entitiesIdsToDelete.toList())
            else emptyList()

        val (entitiesUserCannotAdmin, entitiesUserCanAdmin) =
            entitiesBeforeDelete.partition {
                authorizationService.userCanAdminEntity(it.entityId, sub).isLeft()
            }

        val batchOperationResult = BatchOperationResult().apply {
            addIdsToErrors(unknownEntities, ENTITY_DOES_NOT_EXIST_MESSAGE)
            addIdsToErrors(entitiesUserCannotAdmin.map { it.entityId }, ENTITY_DELETE_FORBIDDEN_MESSAGE)
        }

        if (entitiesUserCanAdmin.isNotEmpty()) {
            val deleteOperationResult =
                entityOperationService.delete(entitiesUserCanAdmin.map { it.entityId }.toSet())

            deleteOperationResult.success.map { it.entityId }.forEach { uri ->
                val entity = entitiesBeforeDelete.find { it.entityId == uri }!!
                entityEventService.publishEntityDeleteEvent(
                    sub.getOrNull(),
                    entity.entityId,
                    entity.types,
                    entity.contexts
                )
            }

            batchOperationResult.errors.addAll(deleteOperationResult.errors)
            batchOperationResult.success.addAll(deleteOperationResult.success)
        }

        if (batchOperationResult.errors.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    private fun checkBatchRequestBody(body: List<Any>): Either<APIException, Unit> =
        if (body.isEmpty())
            BadRequestDataException("Batch request payload shall not be empty").left()
        else Unit.right()

    private suspend fun expandAndPrepareBatchOfEntities(
        payload: List<Map<String, Any>>,
        context: String?,
        contentType: MediaType?
    ): Either<APIException, Pair<List<JsonLdEntity>, List<NgsiLdEntity>>> = either {
        payload
            .let {
                if (contentType == JSON_LD_MEDIA_TYPE)
                    expandJsonLdEntities(it)
                else
                    expandJsonLdEntities(it, listOf(context ?: JsonLdUtils.NGSILD_CORE_CONTEXT))
            }
            .let { jsonLdEntities ->
                Pair(jsonLdEntities, jsonLdEntities.map { it.toNgsiLdEntity().bind() })
            }
    }

    private suspend fun doBatchCreation(
        entitiesToCreate: List<NgsiLdEntity>,
        jsonLdEntities: List<JsonLdEntity>,
        batchOperationResult: BatchOperationResult,
        sub: Option<Sub>
    ) {
        if (entitiesToCreate.isNotEmpty()) {
            val createOperationResult = entityOperationService.create(entitiesToCreate, jsonLdEntities, sub.getOrNull())
            authorizationService.createAdminRights(createOperationResult.getSuccessfulEntitiesIds(), sub)
            entitiesToCreate
                .filter { it.id in createOperationResult.getSuccessfulEntitiesIds() }
                .forEach {
                    entityEventService.publishEntityCreateEvent(
                        sub.getOrNull(),
                        it.id,
                        it.types,
                        it.contexts
                    )
                }
            batchOperationResult.errors.addAll(createOperationResult.errors)
            batchOperationResult.success.addAll(createOperationResult.success)
        }
    }

    private suspend fun publishReplaceEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        ngsiLdEntities: List<NgsiLdEntity>
    ) = ngsiLdEntities.filter { it.id in updateBatchOperationResult.getSuccessfulEntitiesIds() }
        .forEach {
            entityEventService.publishEntityReplaceEvent(
                sub,
                it.id,
                it.types,
                it.contexts
            )
        }

    private suspend fun publishUpdateEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        jsonLdEntities: List<JsonLdEntity>,
        ngsiLdEntities: List<NgsiLdEntity>
    ) {
        updateBatchOperationResult.success.forEach {
            val jsonLdEntity = jsonLdEntities.find { jsonLdEntity -> jsonLdEntity.id.toUri() == it.entityId }!!
            val ngsiLdEntity = ngsiLdEntities.find { ngsiLdEntity -> ngsiLdEntity.id == it.entityId }!!
            entityEventService.publishAttributeChangeEvents(
                sub,
                it.entityId,
                jsonLdEntity.members,
                it.updateResult!!,
                true,
                ngsiLdEntity.contexts
            )
        }
    }
}
