package com.egm.stellio.entity.web

import arrow.core.computations.either
import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntities
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.getSubFromSecurityContext
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
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
            checkContext(httpHeaders, body)
            val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
            val (extractedEntities, _, ngsiLdEntities) =
                expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
            val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)
            val (unauthorizedEntities, authorizedEntities) = newEntities.partition {
                authorizationService.isCreationAuthorized(it, sub).isLeft()
            }
            val batchOperationResult = BatchOperationResult()
            batchOperationResult.addToListOfErrors(existingEntities, "Entity already exists")
            batchOperationResult.addToListOfErrors(unauthorizedEntities, "User forbidden to create entity")

            if (authorizedEntities.isNotEmpty()) {
                val createOperationResult = entityOperationService.create(authorizedEntities)
                authorizationService.createAdminLinks(createOperationResult.getSuccessfulEntitiesIds(), sub)
                ngsiLdEntities
                    .filter { it.id in createOperationResult.getSuccessfulEntitiesIds() }
                    .forEach {
                        val entityPayload = extractEntityPayloadById(extractedEntities, it.id)
                        entityEventService.publishEntityCreateEvent(
                            sub.orNull(),
                            it.id,
                            it.type,
                            extractContextFromInput(entityPayload)
                        )
                    }
                batchOperationResult.errors.addAll(createOperationResult.errors)
                batchOperationResult.success.addAll(createOperationResult.success)
            }

            if (batchOperationResult.errors.isEmpty())
                ResponseEntity.status(HttpStatus.CREATED).body(batchOperationResult.getSuccessfulEntitiesIds())
            else
                ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    private fun extractEntityPayloadById(entitiesPayload: List<Map<String, Any>>, entityId: URI): Map<String, Any> {
        return entitiesPayload.first {
            it["id"] == entityId.toString()
        }
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
            checkContext(httpHeaders, body)
            val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))

            val (extractedEntities, jsonLdEntities, ngsiLdEntities) =
                expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
            val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

            val (newUnauthorizedEntities, newAuthorizedEntities) = newEntities.partition {
                authorizationService.isCreationAuthorized(it, sub).isLeft()
            }
            val createBatchOperationResult = when {
                newAuthorizedEntities.isEmpty() -> BatchOperationResult()
                else -> entityOperationService.create(newAuthorizedEntities)
            }
            createBatchOperationResult.errors.addAll(
                newUnauthorizedEntities.map { BatchEntityError(it.id, arrayListOf("User forbidden to create entity")) }
            )

            authorizationService.createAdminLinks(createBatchOperationResult.getSuccessfulEntitiesIds(), sub)

            val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
                existingEntities.partition { authorizationService.isUpdateAuthorized(it, sub).isLeft() }

            val updateBatchOperationResult = when (options) {
                "update" -> entityOperationService.update(existingEntitiesAuthorized, false)
                else -> entityOperationService.replace(existingEntitiesAuthorized)
            }

            updateBatchOperationResult.errors.addAll(
                existingEntitiesUnauthorized.map {
                    BatchEntityError(it.id, arrayListOf("User forbidden to modify entity"))
                }
            )

            val batchOperationResult = BatchOperationResult(
                ArrayList(createBatchOperationResult.success.plus(updateBatchOperationResult.success)),
                ArrayList(createBatchOperationResult.errors.plus(updateBatchOperationResult.errors))
            )

            ngsiLdEntities
                .filter { it.id in createBatchOperationResult.getSuccessfulEntitiesIds() }
                .forEach {
                    val entityPayload = extractEntityPayloadById(extractedEntities, it.id)
                    entityEventService.publishEntityCreateEvent(
                        sub.orNull(),
                        it.id,
                        it.type,
                        extractContextFromInput(entityPayload)
                    )
                }
            if (options == "update") publishUpdateEvents(sub.orNull(), updateBatchOperationResult, jsonLdEntities)
            else publishReplaceEvents(sub.orNull(), updateBatchOperationResult, extractedEntities, ngsiLdEntities)

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
            checkContext(httpHeaders, body)
            val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
            val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)

            val (_, jsonLdEntities, ngsiLdEntities) =
                expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
            val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

            val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
                existingEntities.partition { authorizationService.isUpdateAuthorized(it, sub).isLeft() }

            val updateBatchOperationResult =
                entityOperationService.update(existingEntitiesAuthorized, disallowOverwrite)
            updateBatchOperationResult.errors.addAll(
                existingEntitiesUnauthorized.map {
                    BatchEntityError(it.id, arrayListOf("User forbidden to modify entity"))
                }
            )
            updateBatchOperationResult.errors.addAll(
                newEntities.map {
                    BatchEntityError(it.id, arrayListOf("Entity does not exist"))
                }
            )
            val batchOperationResult = BatchOperationResult(
                ArrayList(updateBatchOperationResult.success),
                ArrayList(updateBatchOperationResult.errors)
            )
            publishUpdateEvents(sub.orNull(), updateBatchOperationResult, jsonLdEntities)

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

            val (existingEntities, unknownEntities) = entityOperationService
                .splitEntitiesIdsByExistence(body.toListOfUri())

            val entitiesIdsToDelete = existingEntities.toSet()
            val entitiesBeforeDelete = entitiesIdsToDelete.map {
                entityOperationService.getEntityCoreProperties(it)
            }

            val (entitiesUserCannotAdmin, entitiesUserCanAdmin) =
                entitiesBeforeDelete.partition {
                    authorizationService.isAdminAuthorized(it.id, it.type[0], sub).isLeft()
                }

            val batchOperationResult = entityOperationService.delete(entitiesUserCanAdmin.map { it.id }.toSet())
            batchOperationResult.errors.addAll(
                unknownEntities.map { BatchEntityError(it, arrayListOf("Entity does not exist")) }
            )
            batchOperationResult.errors.addAll(
                entitiesUserCannotAdmin.map { BatchEntityError(it.id, arrayListOf("User forbidden to delete entity")) }
            )

            batchOperationResult.success.map { it.entityId }.forEach { uri ->
                val entity = entitiesBeforeDelete.find { it.id == uri }!!
                entityEventService.publishEntityDeleteEvent(sub.orNull(), entity.id, entity.type[0], entity.contexts)
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

    private fun expandAndPrepareBatchOfEntities(
        payload: List<Map<String, Any>>,
        context: String?,
        contentType: MediaType?
    ): Triple<List<Map<String, Any>>, List<JsonLdEntity>, List<NgsiLdEntity>> {
        val jsonldRawEntities =
            if (contentType == JSON_LD_MEDIA_TYPE) payload
            else
                payload.map { rawEntity ->
                    val jsonldRawEntity = rawEntity.toMutableMap()
                    jsonldRawEntity.putIfAbsent(JSONLD_CONTEXT, listOf(context))
                    jsonldRawEntity
                }

        return jsonldRawEntities.let {
            if (contentType == JSON_LD_MEDIA_TYPE)
                Pair(it, expandJsonLdEntities(it))
            else
                Pair(it, expandJsonLdEntities(it, listOf(context!!)))
        }
            .let { Triple(it.first, it.second, it.second.map { it.toNgsiLdEntity() }) }
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
