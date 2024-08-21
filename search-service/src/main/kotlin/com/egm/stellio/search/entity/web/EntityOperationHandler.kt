package com.egm.stellio.search.entity.web

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.service.EntityOperationService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromPostRequest
import com.egm.stellio.search.entity.util.validateMinimalQueryEntitiesParameters
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
    private val entityQueryService: EntityQueryService,
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

        val (parsedEntities, unparsableEntities) = prepareEntitiesFromRequestBody(requestBody, httpHeaders).bind()

        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
        }

        doBatchCreation(parsedEntities, batchOperationResult, sub)

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

        val (parsedEntities, unparsableEntities) = prepareEntitiesFromRequestBody(requestBody, httpHeaders).bind()

        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(parsedEntities)

        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
        }

        val (newUniqueEntities, duplicatedEntities) =
            entityOperationService.splitEntitiesByUniqueness(newEntities)
        val existingOrDuplicatedEntities = existingEntities.plus(duplicatedEntities)

        doBatchCreation(newUniqueEntities, batchOperationResult, sub)

        if (existingOrDuplicatedEntities.isNotEmpty()) {
            val updateOperationResult = when (options) {
                "update" -> entityOperationService.update(existingOrDuplicatedEntities, false, sub.getOrNull())
                else -> entityOperationService.replace(existingOrDuplicatedEntities, sub.getOrNull())
            }

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

        val (parsedEntities, unparsableEntities) = prepareEntitiesFromRequestBody(requestBody, httpHeaders).bind()

        val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)

        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
        }

        if (parsedEntities.isNotEmpty()) {
            val updateOperationResult =
                entityOperationService.update(parsedEntities, disallowOverwrite, sub.getOrNull())

            batchOperationResult.errors.addAll(updateOperationResult.errors)
            batchOperationResult.success.addAll(updateOperationResult.success)
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
     * Implements (6.31.3.1) - Merge Batch of Entities
     */
    @PostMapping("/merge", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun merge(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val (parsedEntities, unparsableEntities) = prepareEntitiesFromRequestBody(requestBody, httpHeaders).bind()

        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
        }

        if (parsedEntities.isNotEmpty()) {
            val mergeOperationResult =
                entityOperationService.merge(parsedEntities, sub.getOrNull())
            batchOperationResult.errors.addAll(mergeOperationResult.errors)
            batchOperationResult.success.addAll(mergeOperationResult.success)
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
     * Implements 6.17.3.1 - Delete Batch of Entities
     */
    @PostMapping("/delete", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun delete(@RequestBody requestBody: Mono<List<String>>): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val body = requestBody.awaitFirst()
        checkBatchRequestBody(body).bind()
        val entitiesId = body.toListOfUri()

        val batchOperationResult = if (entitiesId.isNotEmpty()) {
            val deleteOperationResult = entityOperationService.delete(entitiesId, sub.getOrNull())
            BatchOperationResult(
                errors = deleteOperationResult.errors,
                success = deleteOperationResult.success
            )
        } else { BatchOperationResult() }

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

        val (entities, count) = entityQueryService.queryEntities(entitiesQuery, sub.getOrNull()).bind()

        val filteredEntities = entities.filterAttributes(entitiesQuery.attrs, entitiesQuery.datasetId)

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

    private suspend fun prepareEntitiesFromRequestBody(
        requestBody: Mono<String>,
        httpHeaders: HttpHeaders,
    ): Either<APIException, BatchEntityPreparation> = either {
        val body = requestBody.awaitFirst().deserializeAsList()
            .checkNamesAreNgsiLdSupported().bind()
            .checkContentIsNgsiLdSupported().bind()
        checkBatchRequestBody(body).bind()
        checkContentType(httpHeaders, body).bind()
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()
        expandAndPrepareBatchOfEntities(body, context, httpHeaders.contentType).bind()
    }

    private suspend fun doBatchCreation(
        entitiesToCreate: List<JsonLdNgsiLdEntity>,
        batchOperationResult: BatchOperationResult,
        sub: Option<Sub>
    ) {
        if (entitiesToCreate.isNotEmpty()) {
            val createOperationResult = entityOperationService.create(entitiesToCreate, sub.getOrNull())
            batchOperationResult.errors.addAll(createOperationResult.errors)
            batchOperationResult.success.addAll(createOperationResult.success)
        }
    }
}
