package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadGatewayException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ConflictException
import com.egm.stellio.shared.model.ContextSourceException
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.ErrorType
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

@Service
class DistributedEntityProvisionService(
    private val contextSourceRegistrationService: ContextSourceRegistrationService,
) {
    val entityPath = "/ngsi-ld/v1/entities"

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun distributeDeleteEntity(
        entityId: URI,
        queryParams: MultiValueMap<String, String>
    ): BatchOperationResult {
        val csrFilters = CSRFilters(ids = setOf(entityId), typeSelection = queryParams.getFirst(QP.TYPE.key))
        val result = BatchOperationResult()
        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(filters = csrFilters)

        matchingCSR.filter { it.mode != Mode.AUXILIARY }
            .forEach { csr ->
                if (csr.isMatchingOperation(Operation.DELETE_ENTITY)) {
                    result.addEither(
                        sendDistributedInformation(null, csr, "$entityPath/$entityId", HttpMethod.DELETE),
                        entityId,
                        csr.id
                    )
                } else if (csr.mode != Mode.INCLUSIVE) {
                    result.addEither(
                        ConflictException("csr: ${csr.id} does not support deletion of entities").left(),
                        entityId,
                        csr.id
                    )
                }
            }

        return result
    }

    suspend fun distributeCreateEntity(
        entity: ExpandedEntity,
        contexts: List<String>,
    ): Pair<BatchOperationResult, ExpandedEntity?> = distributeEntityProvision(
        entity,
        contexts,
        Operation.CREATE_ENTITY,
        entity.toTypeSelection()
    )

    suspend fun distributeReplaceEntity(
        entity: ExpandedEntity,
        contexts: List<String>,
        queryParams: MultiValueMap<String, String>
    ): Pair<BatchOperationResult, ExpandedEntity?> = distributeEntityProvision(
        entity,
        contexts,
        Operation.REPLACE_ENTITY,
        queryParams.getFirst(QP.TYPE.key) ?: entity.toTypeSelection()
    )

    suspend fun distributeEntityProvision(
        entity: ExpandedEntity,
        contexts: List<String>,
        operation: Operation,
        typeSelection: EntityTypeSelection
    ): Pair<BatchOperationResult, ExpandedEntity?> {
        val csrFilters =
            CSRFilters(
                ids = setOf(entity.id),
                typeSelection = typeSelection
            )
        val result = BatchOperationResult()

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(
            filters = csrFilters,
        ).groupBy { it.mode }

        val entityAfterExclusive = distributeEntityProvisionForContextSources(
            matchingCSR[Mode.EXCLUSIVE],
            csrFilters,
            entity,
            contexts,
            result,
            operation
        )
        if (entityAfterExclusive == null) return result to null

        val entityAfterRedirect = distributeEntityProvisionForContextSources(
            matchingCSR[Mode.REDIRECT],
            csrFilters,
            entityAfterExclusive,
            contexts,
            result,
            operation
        )
        if (entityAfterRedirect == null) return result to null

        distributeEntityProvisionForContextSources(
            matchingCSR[Mode.INCLUSIVE],
            csrFilters,
            entityAfterRedirect,
            contexts,
            result,
            operation
        )
        return result to entityAfterRedirect
    }

    internal suspend fun distributeEntityProvisionForContextSources(
        csrs: List<ContextSourceRegistration>?,
        csrFilters: CSRFilters,
        entity: ExpandedEntity,
        contexts: List<String>,
        resultToUpdate: BatchOperationResult,
        operation: Operation,
    ): ExpandedEntity? {
        val allProcessedAttrs = mutableSetOf<ExpandedTerm>()
        csrs?.forEach { csr ->
            csr.getAssociatedAttributes(csrFilters, entity)
                .let { attrs ->
                    allProcessedAttrs.addAll(attrs)
                    if (attrs.isEmpty()) Unit
                    else if (csr.isMatchingOperation(operation)) {
                        resultToUpdate.addEither(
                            sendDistributedInformation(
                                compactEntity(entity.filterAttributes(attrs, emptySet()), contexts),
                                csr,
                                operation.getPath(entity.id)!!,
                                operation.method!!
                            ),
                            entity.id,
                            csr.id
                        )
                    } else if (csr.mode != Mode.INCLUSIVE) {
                        resultToUpdate.addEither(
                            ConflictException("The csr: ${csr.id} does not support the creation of entities").left(),
                            entity.id,
                            csr.id
                        )
                    }
                }
        }
        return if (allProcessedAttrs.isNotEmpty()) {
            val remainingEntity = entity.omitAttributes(allProcessedAttrs)
            if (remainingEntity.hasNonCoreAttributes()) remainingEntity else null
        } else entity
    }

    internal suspend fun sendDistributedInformation(
        body: CompactedEntity?,
        csr: ContextSourceRegistration,
        path: String,
        method: HttpMethod,
        queryParams: MultiValueMap<String, String> = MultiValueMap.fromSingleValue(emptyMap())
    ): Either<APIException, Unit> = either {
        val uri = URI("${csr.endpoint}$path")

        val request = WebClient.create()
            .method(method)
            .uri { uriBuilder ->
                uriBuilder.scheme(uri.scheme)
                    .host(uri.host)
                    .port(uri.port)
                    .path(uri.path)
                    .queryParams(queryParams)
                    .build()
            }.headers { newHeaders ->
                newHeaders[HttpHeaders.CONTENT_TYPE] = JSON_LD_CONTENT_TYPE
            }

        body?.let { request.bodyValue(body) }

        return runCatching {
            val (statusCode, response, _) = request.awaitExchange { response ->
                Triple(response.statusCode(), response.awaitBodyOrNull<String>(), response.headers())
            }
            if (statusCode.value() == HttpStatus.MULTI_STATUS.value()) {
                ContextSourceException(
                    type = ErrorType.MULTI_STATUS.type,
                    status = HttpStatus.MULTI_STATUS,
                    title = "Context source returned 207",
                    detail = response ?: "no message"
                ).left()
            } else if (statusCode.is2xxSuccessful) {
                logger.info(
                    "" +
                        "" +
                        "Successfully post data to CSR ${csr.id} at $uri"
                )
                Unit.right()
            } else if (response == null) {
                val message = "No error message received from CSR ${csr.id} at $uri"
                logger.warn(message)
                BadGatewayException(message).left()
            } else {
                logger.warn("Error creating an entity for CSR at $uri: $response")
                ContextSourceException.fromResponse(response).left()
            }
        }.fold(
            onSuccess = { it },
            onFailure = { e ->
                logger.warn("Error contacting CSR at $uri: ${e.message}")
                logger.warn(e.stackTraceToString())
                GatewayTimeoutException(
                    "Error connecting to CSR at $uri: \"${e.cause}:${e.message}\""
                ).left()
            }
        )
    }
}
