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
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.util.composeEntitiesQueryFromPostRequest
import com.egm.stellio.search.util.validateMinimalQueryEntitiesParameters
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntityF
import com.egm.stellio.shared.util.JsonLdUtils.filterJsonLdEntitiesOnAttributes
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
        checkContext(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()
        val (parsedEntities, unparsableEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(parsedEntities)
        val (unauthorizedEntities, authorizedEntities) = newEntities.partition {
            authorizationService.userCanCreateEntities(sub).isLeft()
        }
        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
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
        checkContext(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()

        val (parsedEntities, unparsableEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(parsedEntities)

        val (newUnauthorizedEntities, newAuthorizedEntities) = newEntities.partition {
            authorizationService.userCanCreateEntities(sub).isLeft()
        }
        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
            addEntitiesToErrors(newUnauthorizedEntities.extractNgsiLdEntities(), ENTITIY_CREATION_FORBIDDEN_MESSAGE)
        }

        doBatchCreation(newAuthorizedEntities, batchOperationResult, sub)

        val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
            existingEntities.partition { authorizationService.userCanUpdateEntity(it.entityId(), sub).isLeft() }
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

        if (batchOperationResult.errors.isEmpty() && newEntities.isNotEmpty())
            ResponseEntity.status(HttpStatus.CREATED).body(newEntities.map { it.entityId() })
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

        val (parsedEntities, unparsableEntities) =
            expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(parsedEntities)

        val (existingEntitiesUnauthorized, existingEntitiesAuthorized) =
            existingEntities.partition { authorizationService.userCanUpdateEntity(it.entityId(), sub).isLeft() }

        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
            addEntitiesToErrors(newEntities.extractNgsiLdEntities(), ENTITY_DOES_NOT_EXIST_MESSAGE)
            addEntitiesToErrors(existingEntitiesUnauthorized.extractNgsiLdEntities(), ENTITY_UPDATE_FORBIDDEN_MESSAGE)
        }

        if (existingEntitiesAuthorized.isNotEmpty()) {
            val updateOperationResult =
                entityOperationService.update(existingEntitiesAuthorized, disallowOverwrite, sub.getOrNull())

            publishUpdateEvents(sub.getOrNull(), updateOperationResult, parsedEntities)

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
            val deleteOperationResult = entityOperationService.delete(entitiesUserCanAdmin.map { it.entityId }.toSet())

            deleteOperationResult.success.map { it.entityId }.forEach { uri ->
                val entity = entitiesBeforeDelete.find { it.entityId == uri }!!
                entityEventService.publishEntityDeleteEvent(
                    sub.getOrNull(),
                    entity,
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
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val entitiesQuery = composeEntitiesQueryFromPostRequest(
            applicationProperties.pagination,
            requestBody.awaitFirst(),
            params,
            contextLink
        ).bind()
            .validateMinimalQueryEntitiesParameters().bind()

        val accessRightFilter = authorizationService.computeAccessRightFilter(sub)
        val countAndEntities = queryService.queryEntities(entitiesQuery, accessRightFilter).bind()

        val filteredEntities = filterJsonLdEntitiesOnAttributes(countAndEntities.first, entitiesQuery.attrs)

        val compactedEntities = JsonLdUtils.compactEntities(
            filteredEntities,
            contextLink,
            mediaType
        )

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            countAndEntities.second,
            "/ngsi-ld/v1/entities",
            entitiesQuery.paginationQuery,
            LinkedMultiValueMap(),
            mediaType,
            contextLink
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
    ): BatchEntityPreparation =
        payload.map {
            if (contentType == JSON_LD_MEDIA_TYPE)
                expandJsonLdEntityF(it).mapLeft { apiException -> Pair(it[JSONLD_ID_TERM] as String, apiException) }
            else
                expandJsonLdEntityF(it, listOf(context ?: NGSILD_CORE_CONTEXT))
                    .mapLeft { apiException -> Pair(it[JSONLD_ID_TERM] as String, apiException) }
        }.map {
            when (it) {
                is Either.Left -> it.value.left()
                is Either.Right -> {
                    when (val result = it.value.toNgsiLdEntity()) {
                        is Either.Left -> Pair(it.value.id, result.value).left()
                        is Either.Right -> Pair(it.value, result.value).right()
                    }
                }
            }
        }.fold(BatchEntityPreparation()) { acc, entry ->
            when (entry) {
                is Either.Left -> acc.copy(errors = acc.errors.plus(entry.value))
                is Either.Right -> acc.copy(success = acc.success.plus(entry.value))
            }
        }

    private suspend fun doBatchCreation(
        entitiesToCreate: List<JsonLdNgsiLdEntity>,
        batchOperationResult: BatchOperationResult,
        sub: Option<Sub>
    ) {
        if (entitiesToCreate.isNotEmpty()) {
            val createOperationResult = entityOperationService.create(entitiesToCreate, sub.getOrNull())
            authorizationService.createAdminRights(createOperationResult.getSuccessfulEntitiesIds(), sub)
            entitiesToCreate
                .filter { it.second.id in createOperationResult.getSuccessfulEntitiesIds() }
                .forEach {
                    val ngsiLdEntity = it.second
                    entityEventService.publishEntityCreateEvent(
                        sub.getOrNull(),
                        ngsiLdEntity.id,
                        ngsiLdEntity.types,
                        ngsiLdEntity.contexts
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
                it.second.types,
                it.second.contexts
            )
        }

    private suspend fun publishUpdateEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        jsonLdNgsiLdEntities: List<JsonLdNgsiLdEntity>
    ) {
        updateBatchOperationResult.success.forEach {
            val (jsonLdEntity, ngsiLdEntity) = jsonLdNgsiLdEntities.find { jsonLdNgsiLdEntity ->
                jsonLdNgsiLdEntity.entityId() == it.entityId
            }!!
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
