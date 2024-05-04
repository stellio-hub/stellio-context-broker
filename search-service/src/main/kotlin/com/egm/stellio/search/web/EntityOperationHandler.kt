package com.egm.stellio.search.web

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.model.Query
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityOperationService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.util.composeEntitiesQueryFromPostRequest
import com.egm.stellio.search.util.validateMinimalQueryEntitiesParameters
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntityF
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
class EntityOperationHandler(
    private val applicationProperties: ApplicationProperties,
    private val entityOperationService: EntityOperationService,
    private val entityPayloadService: EntityPayloadService,
    private val queryService: QueryService,
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
        checkContentType(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()

        val (parsedEntities, unparsableEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType).bind()
        val (uniqueEntities, duplicateEntities) =
            entityOperationService.splitEntitiesByUniqueness(parsedEntities)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(uniqueEntities)
        val (unauthorizedEntities, authorizedEntities) = newEntities.partition {
            authorizationService.userCanCreateEntities(sub).isLeft()
        }
        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
            addEntitiesToErrors(duplicateEntities.extractNgsiLdEntities(), ENTITY_ALREADY_EXISTS_MESSAGE)
            addEntitiesToErrors(existingEntities.extractNgsiLdEntities(), ENTITY_ALREADY_EXISTS_MESSAGE)
            addEntitiesToErrors(unauthorizedEntities.extractNgsiLdEntities(), ENTITIY_CREATION_FORBIDDEN_MESSAGE)
        }

        doBatchCreation(authorizedEntities, batchOperationResult, sub)

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
        checkContentType(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()

        val (parsedEntities, unparsableEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType).bind()
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(parsedEntities)

        val (newUnauthorizedEntities, newAuthorizedEntities) = newEntities.partition {
            authorizationService.userCanCreateEntities(sub).isLeft()
        }
        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
            addEntitiesToErrors(newUnauthorizedEntities.extractNgsiLdEntities(), ENTITIY_CREATION_FORBIDDEN_MESSAGE)
        }

        val (newUniqueEntities, duplicatedEntities) =
            entityOperationService.splitEntitiesByUniqueness(newAuthorizedEntities)
        val existingOrDuplicatedEntities = existingEntities.plus(duplicatedEntities)

        doBatchCreation(newUniqueEntities, batchOperationResult, sub)

        val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
            existingOrDuplicatedEntities.partition {
                authorizationService.userCanUpdateEntity(it.entityId(), sub).isLeft()
            }
        batchOperationResult.addEntitiesToErrors(
            existingEntitiesUnauthorized.extractNgsiLdEntities(),
            ENTITY_UPDATE_FORBIDDEN_MESSAGE
        )

        if (existingEntitiesAuthorized.isNotEmpty()) {
            val updateOperationResult = when (options) {
                "update" -> entityOperationService.update(existingEntitiesAuthorized, false, sub.getOrNull())
                else -> entityOperationService.replace(existingEntitiesAuthorized, sub.getOrNull())
            }

            if (options == "update")
                publishUpdateEvents(sub.getOrNull(), updateOperationResult, parsedEntities)
            else
                publishReplaceEvents(sub.getOrNull(), updateOperationResult, parsedEntities)

            batchOperationResult.errors.addAll(updateOperationResult.errors)
            batchOperationResult.success.addAll(updateOperationResult.success)
        }

        if (batchOperationResult.errors.isEmpty() && newUniqueEntities.isNotEmpty())
            ResponseEntity.status(HttpStatus.CREATED).body(newUniqueEntities.map { it.entityId() })
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
        checkContentType(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()
        val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)

        val (parsedEntities, unparsableEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType).bind()
        val (existingEntities, unknownEntities) = entityOperationService.splitEntitiesByExistence(parsedEntities)

        val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
            existingEntities.partition { authorizationService.userCanUpdateEntity(it.entityId(), sub).isLeft() }

        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
            addEntitiesToErrors(unknownEntities.extractNgsiLdEntities(), ENTITY_DOES_NOT_EXIST_MESSAGE)
            addEntitiesToErrors(existingEntitiesUnauthorized.extractNgsiLdEntities(), ENTITY_UPDATE_FORBIDDEN_MESSAGE)
        }

        if (existingEntitiesAuthorized.isNotEmpty()) {
            val updateOperationResult =
                entityOperationService.update(existingEntitiesAuthorized, disallowOverwrite, sub.getOrNull())

            publishUpdateEvents(sub.getOrNull(), updateOperationResult, parsedEntities)

            batchOperationResult.errors.addAll(updateOperationResult.errors)
            batchOperationResult.success.addAll(updateOperationResult.success)
        }

        if (batchOperationResult.errors.isEmpty() && unknownEntities.isEmpty())
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

        val (uniqueEntitiesId, duplicateEntitiesId) =
            entityOperationService.splitEntitiesIdsByUniqueness(body.toListOfUri())

        val (existingEntities, unknownEntities) =
            entityOperationService.splitEntitiesIdsByExistence(uniqueEntitiesId)

        val (entitiesUserCannotDelete, entitiesUserCanDelete) =
            existingEntities.partition {
                authorizationService.userCanAdminEntity(it, sub).isLeft()
            }

        val entitiesBeforeDelete =
            if (entitiesUserCanDelete.isNotEmpty())
                entityPayloadService.retrieve(entitiesUserCanDelete.toList())
            else emptyList()

        val batchOperationResult = BatchOperationResult().apply {
            addIdsToErrors(duplicateEntitiesId, ENTITY_DOES_NOT_EXIST_MESSAGE)
            addIdsToErrors(unknownEntities, ENTITY_DOES_NOT_EXIST_MESSAGE)
            addIdsToErrors(entitiesUserCannotDelete, ENTITY_DELETE_FORBIDDEN_MESSAGE)
        }

        if (entitiesUserCanDelete.isNotEmpty()) {
            val deleteOperationResult = entityOperationService.delete(entitiesUserCanDelete.toSet())

            deleteOperationResult.success.map { it.entityId }.forEach { uri ->
                val entity = entitiesBeforeDelete.find { it.entityId == uri }!!
                entityEventService.publishEntityDeleteEvent(
                    sub.getOrNull(),
                    entity
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

    /**
     * Implements 6.23.3.1 - Query Entities via POST
     */
    @PostMapping("/query", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, GEO_JSON_CONTENT_TYPE])
    suspend fun queryEntitiesViaPost(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val query = Query(requestBody.awaitFirst()).bind()

        val entitiesQuery = composeEntitiesQueryFromPostRequest(
            applicationProperties.pagination,
            query,
            params,
            contexts
        ).bind()
            .validateMinimalQueryEntitiesParameters().bind()

        val accessRightFilter = authorizationService.computeAccessRightFilter(sub)
        val (entities, count) = queryService.queryEntities(entitiesQuery, accessRightFilter).bind()

        val filteredEntities = entities.filterOnAttributes(entitiesQuery.attrs)

        val compactedEntities = compactEntities(filteredEntities, contexts)

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
            .copy(languageFilter = query.lang)

        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            count,
            "/ngsi-ld/v1/entities",
            entitiesQuery.paginationQuery,
            LinkedMultiValueMap(),
            mediaType,
            contexts
        )
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
    ): Either<APIException, BatchEntityPreparation> =
        payload.map {
            val jsonLdExpansionResult =
                if (contentType == JSON_LD_MEDIA_TYPE)
                    expandJsonLdEntityF(
                        it.minus(JSONLD_CONTEXT),
                        addCoreContextIfMissing(it.extractContexts(), applicationProperties.contexts.core)
                    )
                else
                    expandJsonLdEntityF(
                        it,
                        addCoreContextIfMissing(listOfNotNull(context), applicationProperties.contexts.core)
                    )
            jsonLdExpansionResult
                .mapLeft { apiException -> Pair(it[JSONLD_ID_TERM] as String, apiException) }
                .flatMap { jsonLdEntity ->
                    jsonLdEntity.toNgsiLdEntity()
                        .mapLeft { apiException -> Pair(it[JSONLD_ID_TERM] as String, apiException) }
                        .map { ngsiLdEntity -> Pair(jsonLdEntity, ngsiLdEntity) }
                }
        }.fold(BatchEntityPreparation()) { acc, entry ->
            when (entry) {
                is Either.Left -> acc.copy(errors = acc.errors.plus(entry.value))
                is Either.Right -> acc.copy(success = acc.success.plus(entry.value))
            }
        }.let { batchEntityPreparation ->
            // fail fast for LdContextNotAvailableException errors
            batchEntityPreparation.errors.find { it.second is LdContextNotAvailableException }?.second?.left()
                ?: batchEntityPreparation.right()
        }

    private suspend fun doBatchCreation(
        entitiesToCreate: List<JsonLdNgsiLdEntity>,
        batchOperationResult: BatchOperationResult,
        sub: Option<Sub>
    ) {
        if (entitiesToCreate.isNotEmpty()) {
            val createOperationResult = entityOperationService.create(entitiesToCreate, sub.getOrNull())
            authorizationService.createOwnerRights(createOperationResult.getSuccessfulEntitiesIds(), sub)
            entitiesToCreate
                .filter { it.second.id in createOperationResult.getSuccessfulEntitiesIds() }
                .forEach {
                    val ngsiLdEntity = it.second
                    entityEventService.publishEntityCreateEvent(
                        sub.getOrNull(),
                        ngsiLdEntity.id,
                        ngsiLdEntity.types
                    )
                }
            batchOperationResult.errors.addAll(createOperationResult.errors)
            batchOperationResult.success.addAll(createOperationResult.success)
        }
    }

    private suspend fun publishReplaceEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        jsonLdNgsiLdEntities: List<JsonLdNgsiLdEntity>
    ) = jsonLdNgsiLdEntities.filter { it.entityId() in updateBatchOperationResult.getSuccessfulEntitiesIds() }
        .forEach {
            entityEventService.publishEntityReplaceEvent(
                sub,
                it.entityId(),
                it.second.types
            )
        }

    private suspend fun publishUpdateEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        jsonLdNgsiLdEntities: List<JsonLdNgsiLdEntity>
    ) {
        updateBatchOperationResult.success.forEach {
            val (jsonLdEntity, _) = jsonLdNgsiLdEntities.find { jsonLdNgsiLdEntity ->
                jsonLdNgsiLdEntity.entityId() == it.entityId
            }!!
            entityEventService.publishAttributeChangeEvents(
                sub,
                it.entityId,
                jsonLdEntity.members,
                it.updateResult!!,
                true
            )
        }
    }
}
