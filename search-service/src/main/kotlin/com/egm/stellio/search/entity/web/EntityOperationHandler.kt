package com.egm.stellio.search.entity.web

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.service.EntityOperationService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromPost
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.model.NgsiLdDataRepresentation.Companion.parseRepresentations
import com.egm.stellio.shared.model.filterAttributes
import com.egm.stellio.shared.model.parameter.QueryParam
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.GEO_JSON_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntityF
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.addCoreContextIfMissing
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.checkContentIsNgsiLdSupported
import com.egm.stellio.shared.util.checkContentType
import com.egm.stellio.shared.util.checkNamesAreNgsiLdSupported
import com.egm.stellio.shared.util.extractContexts
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeader
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.toListOfUri
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
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

        if (parsedEntities.isNotEmpty()) {
            val createOperationResult = entityOperationService.create(parsedEntities, sub.getOrNull())
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

        val batchOperationResult = BatchOperationResult().apply {
            addEntitiesToErrors(unparsableEntities)
        }

        val newUniqueEntities = if (parsedEntities.isNotEmpty()) {
            val (updateOperationResult, newUniqueEntities) = entityOperationService.upsert(
                parsedEntities,
                options,
                sub.getOrNull()
            )

            batchOperationResult.errors.addAll(updateOperationResult.errors)
            batchOperationResult.success.addAll(updateOperationResult.success)
            newUniqueEntities
        } else emptyList()

        if (batchOperationResult.errors.isEmpty() && newUniqueEntities.isNotEmpty())
            ResponseEntity.status(HttpStatus.CREATED).body(newUniqueEntities.map { it })
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

        val disallowOverwrite = options.map { it == QueryParam.OptionValue.NO_OVERWRITE.value }.orElse(false)

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

        val entitiesQuery = composeEntitiesQueryFromPost(
            applicationProperties.pagination,
            query,
            params,
            contexts
        ).bind()

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
}
