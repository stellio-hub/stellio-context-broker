package com.egm.stellio.entity.web

import arrow.core.Option
import arrow.core.continuations.either
import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntities
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.net.URI
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
class EntityOperationHandler(
    private val entityOperationService: EntityOperationService,
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
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val body = requestBody.awaitFirst().deserializeAsList()
            checkBatchRequestBody(body)
            checkContext(httpHeaders, body)
            val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
            val (_, _, ngsiLdEntities) =
                expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
            val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)
            val (unauthorizedEntities, authorizedEntities) = newEntities.partition {
                authorizationService.isCreationAuthorized(it, sub).isLeft()
            }
            val batchOperationResult = BatchOperationResult().apply {
                addEntitiesToErrors(existingEntities, ENTITY_ALREADY_EXISTS_MESSAGE)
                addEntitiesToErrors(unauthorizedEntities, ENTITIY_CREATION_FORBIDDEN_MESSAGE)
            }

            doBatchCreation(authorizedEntities, ngsiLdEntities, batchOperationResult, sub)

            if (batchOperationResult.errors.isEmpty())
                ResponseEntity.status(HttpStatus.CREATED).body(batchOperationResult.getSuccessfulEntitiesIds())
            else
                ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.15.3.1 - Upsert Batch of Entities
     */
    @PostMapping("/upsert", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun upsert(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @RequestParam(required = false) options: String?
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val body = requestBody.awaitFirst().deserializeAsList()
            checkBatchRequestBody(body)
            checkContext(httpHeaders, body)
            val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))

            val (extractedEntities, jsonLdEntities, ngsiLdEntities) =
                expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
            val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

            val (newUnauthorizedEntities, newAuthorizedEntities) = newEntities.partition {
                authorizationService.isCreationAuthorized(it, sub).isLeft()
            }
            val batchOperationResult = BatchOperationResult().apply {
                addEntitiesToErrors(newUnauthorizedEntities, ENTITIY_CREATION_FORBIDDEN_MESSAGE)
            }

            doBatchCreation(newAuthorizedEntities, ngsiLdEntities, batchOperationResult, sub)

            val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
                existingEntities.partition { authorizationService.isUpdateAuthorized(it, sub).isLeft() }
            batchOperationResult.addEntitiesToErrors(existingEntitiesUnauthorized, ENTITY_UPDATE_FORBIDDEN_MESSAGE)

            if (existingEntitiesAuthorized.isNotEmpty()) {
                val updateOperationResult = when (options) {
                    "update" -> entityOperationService.update(existingEntitiesAuthorized, false)
                    else -> entityOperationService.replace(existingEntitiesAuthorized)
                }

                if (options == "update") publishUpdateEvents(sub.orNull(), updateOperationResult, jsonLdEntities)
                else publishReplaceEvents(sub.orNull(), updateOperationResult, extractedEntities, ngsiLdEntities)

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
    }

    /**
     * Implements 6.16.3.1 - Update Batch of Entities
     */
    @PostMapping("/update", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun update(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @RequestParam options: Optional<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val body = requestBody.awaitFirst().deserializeAsList()
            checkBatchRequestBody(body)
            checkContext(httpHeaders, body)
            val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
            val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)

            val (_, jsonLdEntities, ngsiLdEntities) =
                expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
            val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

            val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
                existingEntities.partition { authorizationService.isUpdateAuthorized(it, sub).isLeft() }

            val batchOperationResult = BatchOperationResult().apply {
                addEntitiesToErrors(newEntities, ENTITY_DOES_NOT_EXIST_MESSAGE)
                addEntitiesToErrors(existingEntitiesUnauthorized, ENTITY_UPDATE_FORBIDDEN_MESSAGE)
            }

            if (existingEntitiesAuthorized.isNotEmpty()) {
                val updateOperationResult =
                    entityOperationService.update(existingEntitiesAuthorized, disallowOverwrite)

                publishUpdateEvents(sub.orNull(), updateOperationResult, jsonLdEntities)

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
    }

    /**
     * Implements 6.17.3.1 - Delete Batch of Entities
     */
    @PostMapping("/delete", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun delete(@RequestBody requestBody: Mono<List<String>>): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            val body = requestBody.awaitFirst()
            checkBatchRequestBody(body)

            val (existingEntities, unknownEntities) =
                entityOperationService.splitEntitiesIdsByExistence(body.toListOfUri())

            val entitiesIdsToDelete = existingEntities.toSet()
            val entitiesBeforeDelete = entitiesIdsToDelete.map {
                entityOperationService.getEntityCoreProperties(it)
            }

            val (entitiesUserCannotAdmin, entitiesUserCanAdmin) =
                entitiesBeforeDelete.partition {
                    authorizationService.isAdminAuthorized(it.id, it.type[0], sub).isLeft()
                }

            val batchOperationResult = BatchOperationResult().apply {
                addIdsToErrors(unknownEntities, ENTITY_DOES_NOT_EXIST_MESSAGE)
                addIdsToErrors(entitiesUserCannotAdmin.map { it.id }, ENTITY_DELETE_FORBIDDEN_MESSAGE)
            }

            if (entitiesUserCanAdmin.isNotEmpty()) {
                val deleteOperationResult = entityOperationService.delete(entitiesUserCanAdmin.map { it.id }.toSet())

                deleteOperationResult.success.map { it.entityId }.forEach { uri ->
                    val entity = entitiesBeforeDelete.find { it.id == uri }!!
                    entityEventService.publishEntityDeleteEvent(
                        sub.orNull(),
                        entity.id,
                        entity.type[0],
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
    }

    private fun checkBatchRequestBody(body: List<Any>) {
        if (body.isEmpty())
            throw BadRequestDataException("Batch request payload shall not be empty")
    }

    private fun expandAndPrepareBatchOfEntities(
        payload: List<Map<String, Any>>,
        context: String?,
        contentType: MediaType?
    ): Triple<List<Map<String, Any>>, List<JsonLdEntity>, List<NgsiLdEntity>> =
        payload.let {
            if (contentType == JSON_LD_MEDIA_TYPE)
                Pair(it, expandJsonLdEntities(it))
            else
                Pair(it, expandJsonLdEntities(it, listOf(context ?: JsonLdUtils.NGSILD_CORE_CONTEXT)))
        }
            .let { Triple(it.first, it.second, it.second.map { it.toNgsiLdEntity() }) }

    private fun extractEntityPayloadById(entitiesPayload: List<Map<String, Any>>, entityId: URI): Map<String, Any> =
        entitiesPayload.first {
            it["id"] == entityId.toString()
        }

    private fun doBatchCreation(
        entitiesToCreate: List<NgsiLdEntity>,
        ngsiLdEntities: List<NgsiLdEntity>,
        batchOperationResult: BatchOperationResult,
        sub: Option<Sub>
    ) {
        if (entitiesToCreate.isNotEmpty()) {
            val createOperationResult = entityOperationService.create(entitiesToCreate)
            authorizationService.createAdminLinks(createOperationResult.getSuccessfulEntitiesIds(), sub)
            ngsiLdEntities
                .filter { it.id in createOperationResult.getSuccessfulEntitiesIds() }
                .forEach {
                    entityEventService.publishEntityCreateEvent(
                        sub.orNull(),
                        it.id,
                        it.type,
                        it.contexts
                    )
                }
            batchOperationResult.errors.addAll(createOperationResult.errors)
            batchOperationResult.success.addAll(createOperationResult.success)
        }
    }

    private fun publishReplaceEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        extractedEntities: List<Map<String, Any>>,
        ngsiLdEntities: List<NgsiLdEntity>
    ) = ngsiLdEntities.filter { it.id in updateBatchOperationResult.getSuccessfulEntitiesIds() }
        .forEach {
            val contexts = extractContextFromInput(extractEntityPayloadById(extractedEntities, it.id))
            entityEventService.publishEntityReplaceEvent(
                sub,
                it.id,
                it.type,
                contexts
            )
        }

    private fun publishUpdateEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        jsonLdEntities: List<JsonLdEntity>
    ) {
        updateBatchOperationResult.success.forEach {
            val jsonLdEntity = jsonLdEntities.find { jsonLdEntity -> jsonLdEntity.id.toUri() == it.entityId }!!
            entityEventService.publishAttributeAppendEvents(
                sub,
                it.entityId,
                jsonLdEntity.properties,
                it.updateResult!!,
                jsonLdEntity.contexts
            )
        }
    }
}
